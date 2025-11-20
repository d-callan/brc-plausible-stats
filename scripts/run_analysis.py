#!/usr/bin/env python3
"""
Run both analysis scripts on the specified input file and generate output files
with names that include the input filename.
"""

import sys
import os
import subprocess

def main():
    if len(sys.argv) < 2:
        print("Usage: python run_analysis.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    base_name = os.path.basename(input_file)
    name_without_ext = os.path.splitext(base_name)[0]
    
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(os.path.dirname(script_dir), 'output')
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Define output filenames in output directory
    organism_output = os.path.join(output_dir, f"{name_without_ext}-organism-analysis.txt")
    workflow_output = os.path.join(output_dir, f"{name_without_ext}-workflow-analysis.txt")
    
    # Get paths to analysis scripts
    organism_script = os.path.join(script_dir, "analyze_organisms.py")
    workflow_script = os.path.join(script_dir, "analyze_workflows.py")
    
    print(f"Running organism analysis on {input_file}...")
    subprocess.run(["python3", organism_script, input_file, organism_output])
    
    print(f"Running workflow analysis on {input_file}...")
    subprocess.run(["python3", workflow_script, input_file, workflow_output])
    
    print("\nAnalysis complete!")
    print(f"Organism analysis output: {organism_output}")
    print(f"Workflow analysis output: {workflow_output}")

if __name__ == "__main__":
    main()
