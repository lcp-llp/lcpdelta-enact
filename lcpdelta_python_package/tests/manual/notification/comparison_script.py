#!/usr/bin/env python3
"""
Notification Order Comparison Script

Compares two notification log files to verify execution order matches.
Generates a simple report with key metrics.

Usage:
    python compare_notifications.py <control_log.jsonl> <test_log.jsonl>
    python compare_notifications.py <control_log.jsonl> <test_log.jsonl> -v
"""

import json
import sys
from collections import defaultdict


def extract_series_identifier(notification):
    """Extract unique identifier from notification data"""
    series = notification.get('series', '')
    option = notification.get('option', '')
    first_choice = notification.get('firstChoice', '')
    
    # Combine series name with option if present
    if option:
        return f"{first_choice}&{option}"
    return first_choice


def parse_log_file(filepath, verbose=False):
    """Parse notification log file and extract execution sequence"""
    executions = []
    line_count = 0
    error_count = 0
    
    if verbose:
        print(f"Reading file: {filepath}", file=sys.stderr)
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line_count += 1
                line = line.strip()
                
                if not line:
                    continue
                
                try:
                    entry = json.loads(line)
                    
                    if 'timestamp' not in entry or 'data' not in entry:
                        error_count += 1
                        continue
                    
                    timestamp = entry['timestamp']
                    data = entry['data']
                    
                    # Handle data as list of notifications
                    if isinstance(data, list):
                        for notification in data:
                            series_id = extract_series_identifier(notification)
                            if series_id:
                                executions.append({
                                    'timestamp': timestamp,
                                    'series': series_id,
                                    'time': notification.get('time', ''),
                                    'value': notification.get('valueHolder', {}).get('value', {}).get('point', {}).get('y')
                                })
                            else:
                                error_count += 1
                    else:
                        error_count += 1
                        
                except (json.JSONDecodeError, KeyError, AttributeError) as e:
                    error_count += 1
                    if verbose:
                        print(f"  Error on line {line_num}: {e}", file=sys.stderr)
                    continue
        
        if verbose:
            print(f"  Lines read: {line_count}, Valid: {len(executions)}, Skipped: {error_count}", file=sys.stderr)
        
    except FileNotFoundError:
        print(f"ERROR: File not found: {filepath}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"ERROR: Failed to read file {filepath}: {e}", file=sys.stderr)
        raise
    
    return executions


def analyze_logs(control_executions, test_executions):
    """Analyze and compare execution sequences"""
    
    total_control = len(control_executions)
    total_test = len(test_executions)
    
    min_length = min(total_control, total_test)
    matches = sum(1 for i in range(min_length) 
                  if control_executions[i]['series'] == test_executions[i]['series'])
    
    control_series = set(e['series'] for e in control_executions)
    test_series = set(e['series'] for e in test_executions)
    all_series = control_series | test_series
    unique_series_count = len(all_series)
    
    control_counts = {}
    test_counts = {}
    
    for e in control_executions:
        control_counts[e['series']] = control_counts.get(e['series'], 0) + 1
    
    for e in test_executions:
        test_counts[e['series']] = test_counts.get(e['series'], 0) + 1
    
    multi_exec_series = set()
    for series in all_series:
        if control_counts.get(series, 0) > 1 or test_counts.get(series, 0) > 1:
            multi_exec_series.add(series)
    
    multi_exec_count = len(multi_exec_series)
    order_matches = (total_control == total_test and matches == total_control)
    
    # Find first mismatch
    first_mismatch = None
    if not order_matches:
        for i in range(min_length):
            if control_executions[i]['series'] != test_executions[i]['series']:
                first_mismatch = {
                    'index': i,
                    'control': control_executions[i]['series'],
                    'test': test_executions[i]['series']
                }
                break
    
    return {
        'total_control': total_control,
        'total_test': total_test,
        'matches': matches,
        'min_length': min_length,
        'unique_series': unique_series_count,
        'multi_exec_count': multi_exec_count,
        'multi_exec_series': sorted(multi_exec_series),
        'order_matches': order_matches,
        'match_percentage': (matches / min_length * 100) if min_length > 0 else 0,
        'first_mismatch': first_mismatch
    }


def generate_report(analysis):
    """Generate a simple text report"""
    
    lines = []
    lines.append("=" * 60)
    lines.append("NOTIFICATION ORDER COMPARISON REPORT")
    lines.append("=" * 60)
    lines.append("")
    
    if analysis['order_matches']:
        lines.append("✓ NOTIFICATION ORDER IS IDENTICAL")
    else:
        lines.append("✗ NOTIFICATION ORDER DIFFERS")
    lines.append("")
    
    lines.append("KEY METRICS:")
    lines.append("-" * 60)
    
    if analysis['total_control'] == analysis['total_test']:
        lines.append(f"• Total Notifications: {analysis['total_control']} in both logs")
    else:
        lines.append(f"• Total Notifications: Control={analysis['total_control']}, Test={analysis['total_test']}")
    
    match_ratio = f"{analysis['matches']}/{analysis['min_length']}"
    match_pct = f"({analysis['match_percentage']:.1f}%)"
    lines.append(f"• Sequential Matches: {match_ratio} {match_pct}")
    lines.append(f"• Unique Series: {analysis['unique_series']}")
    lines.append(f"• Series with Multiple Notifications: {analysis['multi_exec_count']}")
    
    if analysis['first_mismatch']:
        lines.append("")
        lines.append("FIRST MISMATCH:")
        lines.append(f"  Position: {analysis['first_mismatch']['index']}")
        lines.append(f"  Control:  {analysis['first_mismatch']['control']}")
        lines.append(f"  Test:     {analysis['first_mismatch']['test']}")
    
    if analysis['multi_exec_series']:
        lines.append("")
        lines.append("  Series with multiple notifications:")
        for series in analysis['multi_exec_series'][:10]:  # Limit to first 10
            lines.append(f"    - {series}")
        if len(analysis['multi_exec_series']) > 10:
            lines.append(f"    ... and {len(analysis['multi_exec_series']) - 10} more")
    
    lines.append("")
    lines.append("=" * 60)
    
    return "\n".join(lines)


def main():
    verbose = '-v' in sys.argv or '--verbose' in sys.argv
    args = [arg for arg in sys.argv[1:] if arg not in ['-v', '--verbose']]
    
    if len(args) != 2:
        print("Usage: python compare_notifications.py <control_log.jsonl> <test_log.jsonl> [-v]", file=sys.stderr)
        sys.exit(1)
    
    control_file = args[0]
    test_file = args[1]
    
    try:
        control_executions = parse_log_file(control_file, verbose)
        test_executions = parse_log_file(test_file, verbose)
        
        if len(control_executions) == 0:
            print(f"ERROR: No valid notifications found in control log: {control_file}", file=sys.stderr)
            sys.exit(2)
        
        if len(test_executions) == 0:
            print(f"ERROR: No valid notifications found in test log: {test_file}", file=sys.stderr)
            sys.exit(2)
        
        if verbose:
            print("", file=sys.stderr)
        
        analysis = analyze_logs(control_executions, test_executions)
        report = generate_report(analysis)
        print(report)
        
        sys.exit(0 if analysis['order_matches'] else 1)
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()