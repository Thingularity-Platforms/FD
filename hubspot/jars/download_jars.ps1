# Run this once after cloning the repo to download required JARs
# Usage: .\jars\download_jars.ps1

$jarsPath = "$PSScriptRoot"

Write-Host "Downloading Kafka + Delta JARs..."

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar" -OutFile "$jarsPath\spark-sql-kafka-0-10_2.12-3.4.0.jar"

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar" -OutFile "$jarsPath\kafka-clients-3.3.2.jar"

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar" -OutFile "$jarsPath\spark-token-provider-kafka-0-10_2.12-3.4.0.jar"

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -OutFile "$jarsPath\commons-pool2-2.11.1.jar"

Write-Host "All JARs downloaded successfully."