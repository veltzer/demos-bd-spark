# Method 1: Check for Spark master process
function check_spark_master() {
    if pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
        echo "Spark master is running"
        return 0
    else
        echo "Spark master is not running"
        return 1
    fi
}

# Method 2: Check default Spark web UI port (8080)
function check_spark_ui() {
    if nc -z localhost 8080 2>/dev/null; then
        echo "Spark UI port is accessible"
        return 0
    else
        echo "Spark UI port is not accessible"
        return 1
    fi
}

# Method 3: Make an HTTP request to Spark master UI
function check_spark_http() {
    if curl -s "http://localhost:8080" | grep -q "Spark Master"; then
        echo "Spark master is responding"
        return 0
    else
        echo "Spark master is not responding"
        return 1
    fi
}
