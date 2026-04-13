terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------
# VPC
# ---------------------------------------------------------------------------
data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.cluster_name}-vpc"
  }
}

resource "aws_subnet" "bench" {
  count                   = 3
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = cidrsubnet(aws_vpc.bench.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name                                        = "${var.cluster_name}-subnet-${count.index}"
    "kubernetes.io/cluster/${var.cluster_name}"  = "shared"
    "kubernetes.io/role/elb"                     = "1"
  }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id

  tags = {
    Name = "${var.cluster_name}-igw"
  }
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }

  tags = {
    Name = "${var.cluster_name}-rt"
  }
}

resource "aws_route_table_association" "bench" {
  count          = 3
  subnet_id      = aws_subnet.bench[count.index].id
  route_table_id = aws_route_table.bench.id
}

# ---------------------------------------------------------------------------
# IAM – EKS cluster role
# ---------------------------------------------------------------------------
resource "aws_iam_role" "eks_cluster" {
  name = "${var.cluster_name}-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

# ---------------------------------------------------------------------------
# IAM – node group role
# ---------------------------------------------------------------------------
resource "aws_iam_role" "eks_nodes" {
  name = "${var.cluster_name}-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_worker" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "node_cni" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "node_ecr" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "node_ebs" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

# ---------------------------------------------------------------------------
# EKS cluster
# ---------------------------------------------------------------------------
resource "aws_eks_cluster" "bench" {
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster.arn
  version  = "1.31"

  vpc_config {
    subnet_ids = aws_subnet.bench[*].id
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
  ]
}

# ---------------------------------------------------------------------------
# OIDC provider for IRSA (IAM Roles for Service Accounts)
# ---------------------------------------------------------------------------
data "tls_certificate" "eks" {
  url = aws_eks_cluster.bench.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.bench.identity[0].oidc[0].issuer
}

# ---------------------------------------------------------------------------
# IAM role for EBS CSI driver (via IRSA)
# ---------------------------------------------------------------------------
resource "aws_iam_role" "ebs_csi" {
  name = "${var.cluster_name}-ebs-csi-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${replace(aws_eks_cluster.bench.identity[0].oidc[0].issuer, "https://", "")}:sub" = "system:serviceaccount:kube-system:ebs-csi-controller-sa"
          "${replace(aws_eks_cluster.bench.identity[0].oidc[0].issuer, "https://", "")}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi_policy" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

# ---------------------------------------------------------------------------
# EBS CSI driver addon
# ---------------------------------------------------------------------------
resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = aws_eks_cluster.bench.name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = aws_iam_role.ebs_csi.arn

  depends_on = [aws_eks_node_group.bench, aws_iam_role_policy_attachment.ebs_csi_policy]
}

# ---------------------------------------------------------------------------
# Node group
# ---------------------------------------------------------------------------
resource "aws_eks_node_group" "bench" {
  cluster_name    = aws_eks_cluster.bench.name
  node_group_name = "${var.cluster_name}-nodes"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = [aws_subnet.bench[0].id]

  instance_types = [var.node_instance_type]

  scaling_config {
    min_size     = 0
    max_size     = 24
    desired_size = 1
  }

  labels = {
    benchmark = "true"
  }

  depends_on = [
    aws_iam_role_policy_attachment.node_worker,
    aws_iam_role_policy_attachment.node_cni,
    aws_iam_role_policy_attachment.node_ecr,
    aws_iam_role_policy_attachment.node_ebs,
  ]
}

# ---------------------------------------------------------------------------
# S3 bucket for benchmark data
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "bench_data" {
  bucket        = var.s3_bucket_name
  force_destroy = true

  tags = {
    Name    = var.s3_bucket_name
    Purpose = "benchmark"
  }
}

resource "aws_iam_policy" "node_s3_access" {
  name        = "${var.cluster_name}-node-s3-access"
  description = "Allow EKS nodes full access to the benchmark S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ]
      Resource = [
        aws_s3_bucket.bench_data.arn,
        "${aws_s3_bucket.bench_data.arn}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_s3" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = aws_iam_policy.node_s3_access.arn
}

# ---------------------------------------------------------------------------
# ECR repositories for custom Airflow images
# ---------------------------------------------------------------------------
resource "aws_ecr_repository" "airflow_pandas" {
  name                 = "${var.cluster_name}/airflow-pandas"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_repository" "airflow_snowflake" {
  name                 = "${var.cluster_name}/airflow-snowflake"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}
