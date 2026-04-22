import json
import os
import subprocess
from datetime import datetime
from pathlib import Path

def run_dbt_tests() -> dict:
    """Run dbt tests and capture results."""
    result = subprocess.run(
        ["dbt", "test", "--output", "json"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent / "dbt_project" / "health_pipeline"
    )
    return result.stdout, result.returncode

def parse_test_results() -> list:
    """Parse dbt test results from target folder."""
    results_path = Path(__file__).parent.parent / "dbt_project" / "health_pipeline" / "target" / "run_results.json"
    
    with open(results_path) as f:
        data = json.load(f)
    
    tests = []
    for result in data.get("results", []):
        tests.append({
            "test_name": result["unique_id"].split(".")[-1],
            "status": result["status"],
            "execution_time": round(result["execution_time"], 2),
            "failures": result.get("failures", 0),
            "message": result.get("message", "")
        })
    
    return tests

def generate_html_report(tests: list) -> str:
    """Generate HTML report from test results."""
    passed = [t for t in tests if t["status"] == "pass"]
    failed = [t for t in tests if t["status"] == "fail"]
    
    rows = ""
    for test in tests:
        color = "#2ecc71" if test["status"] == "pass" else "#e74c3c"
        rows += f"""
        <tr>
            <td>{test["test_name"]}</td>
            <td style="color: {color}; font-weight: bold;">{test["status"].upper()}</td>
            <td>{test["execution_time"]}s</td>
            <td>{test["failures"]}</td>
            <td>{test["message"]}</td>
        </tr>
        """
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>dbt Test Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
            h1 {{ color: #2c3e50; }}
            .summary {{ display: flex; gap: 20px; margin-bottom: 30px; }}
            .card {{ padding: 20px; border-radius: 8px; color: white; min-width: 150px; text-align: center; }}
            .pass {{ background: #2ecc71; }}
            .fail {{ background: #e74c3c; }}
            .total {{ background: #3498db; }}
            .card h2 {{ margin: 0; font-size: 2em; }}
            .card p {{ margin: 5px 0 0 0; }}
            table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }}
            th {{ background: #2c3e50; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 12px; border-bottom: 1px solid #eee; }}
            tr:hover {{ background: #f9f9f9; }}
            .timestamp {{ color: #888; font-size: 0.9em; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h1>Health Pipeline — dbt Test Report</h1>
        <p class="timestamp">Generated at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        
        <div class="summary">
            <div class="card total">
                <h2>{len(tests)}</h2>
                <p>Total Tests</p>
            </div>
            <div class="card pass">
                <h2>{len(passed)}</h2>
                <p>Passed</p>
            </div>
            <div class="card fail">
                <h2>{len(failed)}</h2>
                <p>Failed</p>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Test Name</th>
                    <th>Status</th>
                    <th>Execution Time</th>
                    <th>Failures</th>
                    <th>Message</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </body>
    </html>
    """
    
    return html

def run():
    """Main function to run tests and generate report."""
    print("Running dbt tests...")
    stdout, returncode = run_dbt_tests()
    
    print("Parsing test results...")
    tests = parse_test_results()
    
    print("Generating HTML report...")
    html = generate_html_report(tests)
    
    report_path = Path(__file__).parent / "test_report.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report saved to {report_path}")
    
    # Open in browser
    os.startfile(report_path)

if __name__ == "__main__":
    run()