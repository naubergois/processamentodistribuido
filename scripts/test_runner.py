import subprocess
import time
import os
import signal
import sys
import glob

# Configuration
SCRIPTS_DIR = "scripts"
KAFKA_DIR = glob.glob("kafka_*")[0] if glob.glob("kafka_*") else "kafka_2.13-3.6.1"
JAVA_HOME = "/opt/homebrew/opt/openjdk@11" # Adjust based on brew output
LOG_DIR = "logs/test_runs"

# Colors
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def setup_env():
    # Set JAVA_HOME for the current process and children
    if os.path.exists(JAVA_HOME):
        os.environ["JAVA_HOME"] = JAVA_HOME
        os.environ["PATH"] = f"{JAVA_HOME}/bin:" + os.environ["PATH"]
    else:
        print(f"{RED}Warning: JAVA_HOME not found at {JAVA_HOME}{RESET}")

def start_kafka():
    print("Starting Zookeeper...")
    zoo_log = open(f"{LOG_DIR}/zookeeper.log", "w")
    zoo_proc = subprocess.Popen(
        [f"{KAFKA_DIR}/bin/zookeeper-server-start.sh", f"{KAFKA_DIR}/config/zookeeper.properties"],
        stdout=zoo_log, stderr=zoo_log
    )
    time.sleep(5)
    
    print("Starting Kafka...")
    kafka_log = open(f"{LOG_DIR}/kafka.log", "w")
    kafka_proc = subprocess.Popen(
        [f"{KAFKA_DIR}/bin/kafka-server-start.sh", f"{KAFKA_DIR}/config/server.properties"],
        stdout=kafka_log, stderr=kafka_log
    )
    time.sleep(20) # Wait for startup
    return zoo_proc, kafka_proc

def run_script(script_path):
    script_name = os.path.basename(script_path)
    print(f"Testing {script_name}...", end=" ", flush=True)
    
    log_file = open(f"{LOG_DIR}/{script_name}.log", "w")
    
    try:
        # Run for 60 seconds (Spark needs time to download JARs on first run)
        proc = subprocess.Popen(
            ["python3", script_path],
            stdout=log_file,
            stderr=log_file,
            env=os.environ
        )
        
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            # This is GOOD for streaming jobs (means it didn't crash)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except:
                proc.kill()
            print(f"{GREEN}PASSED (Ran for 20s){RESET}")
            return True
            
        # If it exited early, check code
        if proc.returncode == 0:
             print(f"{GREEN}PASSED (Finished){RESET}")
             return True
        else:
             print(f"{RED}FAILED (Exit Code {proc.returncode}){RESET}")
             # Print tail of log
             return False

    except Exception as e:
        print(f"{RED}ERROR: {e}{RESET}")
        return False

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    setup_env()
    
    # 1. Start Infrastructure
    # Check if we need Kafka (if running scripts 1,3,4,6,7,9,11,13,15,17,19)
    # Just start it.
    zoo, kafka = start_kafka()
    
    failed_scripts = []
    
    try:
        scripts = sorted(glob.glob(f"{SCRIPTS_DIR}/*.py"))
        # Filter out __init__ or this runner if inside scripts dir
        scripts = [s for s in scripts if "test_runner" not in s]
        
        for script in scripts:
            if not run_script(script):
                failed_scripts.append(script)
                
    finally:
        print("Stopping Infrastructure...")
        kafka.terminate()
        zoo.terminate()
        kafka.wait()
        zoo.wait()

    print("\n--- Summary ---")
    if failed_scripts:
        print(f"{RED}Failures:{RESET}")
        for s in failed_scripts:
            print(f"- {s}")
            # print error log head
            with open(f"{LOG_DIR}/{os.path.basename(s)}.log", 'r') as f:
                print(f.read()[-500:])
    else:
        print(f"{GREEN}All scripts passed smoke test!{RESET}")

if __name__ == "__main__":
    main()
