"""HTML audit report generator.

Produces a self-contained HTML page summarizing run statistics
from one or more pipeline runs. Designed for daily audit reports
that can be saved locally or attached to email alerts.
"""

from datetime import datetime
from pathlib import Path

from app.base.types import DatasetDetail, RunStats, ScrapeResult
from app.core.logging import logger


def generate_report(
    stats_list: list[RunStats],
    output_path: Path | None = None,
) -> str:
    """Generate an HTML audit report from pipeline run statistics.

    Args:
        stats_list: List of RunStats from one or more pipeline runs.
        output_path: If provided, writes the HTML to this file.

    Returns:
        The HTML string.
    """
    now = datetime.now()
    total_pipelines = len(stats_list)
    total_scrapes = sum(s.total for s in stats_list)
    total_succeeded = sum(s.succeeded for s in stats_list)
    total_failed = sum(s.failed for s in stats_list)
    total_duration = sum(s.duration_s for s in stats_list)
    success_rate = (total_succeeded / total_scrapes * 100) if total_scrapes > 0 else 0

    pipeline_rows = ""
    for s in stats_list:
        rate = (s.succeeded / s.total * 100) if s.total > 0 else 0
        status_class = "success" if s.failed == 0 else "warning"
        pipeline_rows += f"""
        <tr class="{status_class}">
            <td>{s.pipeline}</td>
            <td>{s.start_time:%Y-%m-%d %H:%M:%S}</td>
            <td>{f"{s.end_time:%Y-%m-%d %H:%M:%S}" if s.end_time else "—"}</td>
            <td>{s.duration_s:.1f}s</td>
            <td>{s.total}</td>
            <td>{s.succeeded}</td>
            <td>{s.failed}</td>
            <td>{rate:.1f}%</td>
        </tr>"""

    # Collect all failures across pipelines
    all_failures: list[ScrapeResult] = []
    for s in stats_list:
        all_failures.extend(s.failures)

    failure_rows = ""
    for f in all_failures:
        failure_rows += f"""
        <tr>
            <td>{f.pipe_code}</td>
            <td>{f.dataset_type.value}</td>
            <td>{f.date:%Y-%m-%d}</td>
            <td>{f.duration_s:.1f}s</td>
            <td class="error-text">{f.error or "Unknown"}</td>
        </tr>"""

    failure_section = ""
    if all_failures:
        failure_section = f"""
    <h2>Failed Scrapes ({len(all_failures)})</h2>
    <table>
        <thead>
            <tr>
                <th>Pipe Code</th>
                <th>Type</th>
                <th>Date</th>
                <th>Duration</th>
                <th>Error</th>
            </tr>
        </thead>
        <tbody>{failure_rows}
        </tbody>
    </table>"""

    # Dataset details table
    all_details: list[DatasetDetail] = []
    for s in stats_list:
        all_details.extend(s.dataset_details)

    missing_details = [d for d in all_details if d.missing]
    present_details = [d for d in all_details if not d.missing]

    dataset_section = ""
    if present_details:
        detail_rows = ""
        for d in present_details:
            new_locs_cell = str(d.new_locations) if d.new_locations > 0 else "—"
            # Build file-level links for raw (ADLS)
            if d.raw_paths:
                raw_links = ", ".join(
                    f'<a href="{p}" target="_blank">{p.rsplit("/", 1)[-1]}</a>'
                    for p in d.raw_paths
                )
            else:
                raw_links = "—"
            # Build file-level links for silver (ADLS)
            if d.silver_paths:
                silver_links = ", ".join(
                    f'<a href="{p}" target="_blank">{p.rsplit("/", 1)[-1]}</a>'
                    for p in d.silver_paths
                )
            else:
                silver_links = "—"
            detail_rows += f"""
        <tr>
            <td>{d.dataset_type.value}</td>
            <td>{d.pipe_code}</td>
            <td>{d.raw_records:,}</td>
            <td>{d.silver_records:,}</td>
            <td>{new_locs_cell}</td>
            <td>{raw_links}</td>
            <td>{silver_links}</td>
        </tr>"""

        dataset_section = f"""
    <h2>Dataset Details ({len(present_details)})</h2>
    <table>
        <thead>
            <tr>
                <th>Type</th>
                <th>Pipe Code</th>
                <th>Raw Records</th>
                <th>Silver Records</th>
                <th>New Locations</th>
                <th>Raw Files</th>
                <th>Silver Files</th>
            </tr>
        </thead>
        <tbody>{detail_rows}
        </tbody>
    </table>"""

    missing_section = ""
    if missing_details:
        missing_rows = ""
        for d in missing_details:
            missing_rows += f"""
        <tr class="warning">
            <td>{d.dataset_type.value}</td>
            <td>{d.pipe_code}</td>
        </tr>"""

        missing_section = f"""
    <h2>Missing Downloads ({len(missing_details)})</h2>
    <table>
        <thead>
            <tr>
                <th>Type</th>
                <th>Pipe Code</th>
            </tr>
        </thead>
        <tbody>{missing_rows}
        </tbody>
    </table>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Audit Report — {now:%Y-%m-%d}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            padding: 24px;
        }}
        .container {{ max-width: 960px; margin: 0 auto; }}
        h1 {{
            font-size: 1.5rem;
            margin-bottom: 8px;
            color: #1a1a2e;
        }}
        .timestamp {{
            font-size: 0.85rem;
            color: #666;
            margin-bottom: 24px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 32px;
        }}
        .card {{
            background: #fff;
            border-radius: 8px;
            padding: 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
            text-align: center;
        }}
        .card .value {{
            font-size: 1.8rem;
            font-weight: 700;
            color: #1a1a2e;
        }}
        .card .label {{
            font-size: 0.75rem;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 4px;
        }}
        .card.green .value {{ color: #2e7d32; }}
        .card.red .value {{ color: #c62828; }}
        .card.blue .value {{ color: #1565c0; }}
        h2 {{
            font-size: 1.1rem;
            margin: 24px 0 12px;
            color: #1a1a2e;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #fff;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
            margin-bottom: 24px;
        }}
        th {{
            background: #1a1a2e;
            color: #fff;
            padding: 10px 12px;
            text-align: left;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #eee;
            font-size: 0.85rem;
        }}
        tr:last-child td {{ border-bottom: none; }}
        tr.success {{ background: #e8f5e9; }}
        tr.warning {{ background: #fff3e0; }}
        .error-text {{
            color: #c62828;
            font-family: monospace;
            font-size: 0.8rem;
            max-width: 300px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        a {{ color: #1565c0; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .footer {{
            text-align: center;
            font-size: 0.75rem;
            color: #999;
            margin-top: 32px;
            padding-top: 16px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
<div class="container">
    <h1>Pipeline Audit Report</h1>
    <p class="timestamp">Generated: {now:%Y-%m-%d %H:%M:%S CT}</p>

    <div class="summary">
        <div class="card blue">
            <div class="value">{total_pipelines}</div>
            <div class="label">Pipelines</div>
        </div>
        <div class="card">
            <div class="value">{total_scrapes}</div>
            <div class="label">Total Scrapes</div>
        </div>
        <div class="card green">
            <div class="value">{total_succeeded}</div>
            <div class="label">Succeeded</div>
        </div>
        <div class="card red">
            <div class="value">{total_failed}</div>
            <div class="label">Failed</div>
        </div>
        <div class="card">
            <div class="value">{success_rate:.1f}%</div>
            <div class="label">Success Rate</div>
        </div>
        <div class="card">
            <div class="value">{total_duration:.1f}s</div>
            <div class="label">Total Duration</div>
        </div>
    </div>

    <h2>Pipeline Summary</h2>
    <table>
        <thead>
            <tr>
                <th>Pipeline</th>
                <th>Start</th>
                <th>End</th>
                <th>Duration</th>
                <th>Total</th>
                <th>OK</th>
                <th>Fail</th>
                <th>Rate</th>
            </tr>
        </thead>
        <tbody>{pipeline_rows}
        </tbody>
    </table>

    {failure_section}

    {dataset_section}

    {missing_section}

    <div class="footer">
        GFScrapePackage Audit Report &mdash; Auto-generated
    </div>
</div>
</body>
</html>"""

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding="utf-8")
        logger.info(f"Audit report written to {output_path}")

    return html
