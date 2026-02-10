"""Generate architecture diagram using the `diagrams` library.

Run: python architecture.py
Outputs: architecture.png
"""

from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.compute import Run
from diagrams.gcp.devtools import Scheduler
from diagrams.gcp.storage import GCS
from diagrams.onprem.client import Users
from diagrams.programming.language import Python

with Diagram(
    "Arqtic Weather Pipeline",
    show=False,
    filename="architecture",
    direction="LR",
):
    api = Python("Open-Meteo\nAPI")

    with Cluster("GCP (Terraform-managed)"):
        registry = GCS("Artifact\nRegistry")

        with Cluster("Data Pipeline"):
            scheduler = Scheduler("Cloud\nScheduler\n(every 6h)")
            pipeline = Run("Pipeline\nCloud Run Job")

        bucket = GCS("Weather Data\nGCS Bucket\n(Parquet)")

        dashboard = Run("Dashboard\nCloud Run Service\n(Streamlit)")

    users = Users("Users")

    api >> Edge(label="Extract") >> pipeline
    scheduler >> Edge(label="Trigger") >> pipeline
    pipeline >> Edge(label="Load") >> bucket
    bucket >> Edge(label="Read") >> dashboard
    users >> Edge(label="View") >> dashboard
