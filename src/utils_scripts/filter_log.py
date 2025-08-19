# /src/utils_scripts/filter_log.py

import re
import sys
import logging

def filter_log(filename):
    matching_lines = []
    pattern = r'Expected (\d+), found (\d+)'
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            match = re.search(pattern, line)
            if match:
                expected = int(match.group(1))
                found = int(match.group(2))
                if expected != found:
                    # Extract specific parts: id, ext_id, expected, found
                    extract_pattern = r'id:\s*(\d+),\s*ext_id:\s*(\d+)\)\.\s*Expected\s*(\d+),\s*found\s*(\d+)'
                    extract_match = re.search(extract_pattern, line)
                    if extract_match:
                        id_val = extract_match.group(1)
                        ext_id = extract_match.group(2)
                        expected_val = extract_match.group(3)
                        found_val = extract_match.group(4)
                        url = f"https://resultat.ondata.se/ViewClassPDF.php?classID={ext_id}&stage=1"
                        extracted = f"{id_val}, {ext_id}, {expected_val}, {found_val}, {url}"
                        matching_lines.append(extracted)
    return matching_lines

if __name__ == "__main__":
    filename = '../../data/logs/log.log'  # Hardcoded as per your path; adjust if needed
    output_filename = 'mismatched_lines.txt'  # Output file; adjust if needed

    try:
        lines = filter_log(filename)
        if lines:
            with open(output_filename, 'w', encoding='utf-8') as out_f:
                out_f.write(f"Found {len(lines)} mismatched lines:\n")
                for line in lines:
                    out_f.write(line + '\n')
            print(f"Results written to {output_filename}")
        else:
            print("No mismatched lines found.")
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except Exception as e:
        print(f"Error: {e}")