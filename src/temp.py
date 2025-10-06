# GROK

import requests
from io import BytesIO
import pdfplumber
import pandas as pd
import re
from collections import defaultdict
import numpy as np

def parse_tournament_bracket(url):
    # Download the PDF
    response = requests.get(url)
    pdf_file = BytesIO(response.content)

    with pdfplumber.open(pdf_file) as pdf:
        page = pdf.pages[0]
        words = page.extract_words(keep_blank_chars=False, use_text_flow=False, extra_attrs=["fontname", "size"])

    # Create DataFrame for words with coordinates
    df = pd.DataFrame(words)
    df['center_y'] = (df['top'] + df['bottom']) / 2
    df['center_x'] = (df['x0'] + df['x1']) / 2

    # Classify text as player or score
    def is_score(text):
        return re.match(r'^[-0-9, ]+$', text) and ',' in text

    df['type'] = df['text'].apply(lambda t: 'score' if is_score(t) else 'player' if re.match(r'^[A-Za-zÉÄÖåäö -]+$', t) else 'other')

    # Filter only player and score
    df_bracket = df[df['type'].isin(['player', 'score'])]

    # Cluster x positions to find columns
    from sklearn.cluster import KMeans
    x_centers = df_bracket['center_x'].values.reshape(-1, 1)
    n_clusters = min(8, len(x_centers))  # up to 4 rounds
    kmeans = KMeans(n_clusters=n_clusters, n_init=10)
    kmeans.fit(x_centers)
    df_bracket['column'] = kmeans.labels_

    # Determine column types
    col_types = df_bracket.groupby('column')['type'].agg(lambda x: x.mode()[0] if not x.empty else 'unknown')

    player_cols = sorted(col_types[col_types == 'player'].index)
    score_cols = sorted(col_types[col_types == 'score'].index)

    # Pair rounds
    rounds = list(zip(player_cols, score_cols)) if len(player_cols) == len(score_cols) else []

    # Extract full player info
    df_full = df[(df['type'] == 'other') & (df['top'] > page.height / 2)]
    full_lines = defaultdict(list)
    for _, row in df_full.sort_values(['top', 'center_x']).iterrows():
        key = round(row['top'], 0)
        full_lines[key].append(row['text'])

    player_club = {}
    for line in full_lines.values():
        line_text = ' '.join(line)
        if ',' in line_text:
            try:
                full_name, club = line_text.split(',', 1)
                full_name = full_name.strip()
                club = club.strip()
                parts = full_name.split()
                surname = parts[0]
                initial = parts[1][0] if len(parts) > 1 else ''
                short = f"{surname} {initial}".strip()
                player_club[short] = (full_name, club)
            except:
                pass

    # Parse matches
    matches = []
    round_names = ['Round of 16', 'Quarterfinals', 'Semifinals', 'Final']

    line_height = df['size'].mean() if not df.empty else 10
    threshold = line_height * 1.5

    for r in range(len(rounds)):
        p_col, s_col = rounds[r]
        df_players = df_bracket[(df_bracket['column'] == p_col) & (df_bracket['type'] == 'player')].sort_values('top')
        df_scores = df_bracket[(df_bracket['column'] == s_col) & (df_bracket['type'] == 'score')].sort_values('center_y')

        # Group players into matches
        match_groups = []
        current_group = []
        for _, row in df_players.iterrows():
            if current_group and row['top'] - current_group[-1]['top'] > threshold:
                match_groups.append(current_group)
                current_group = []
            current_group.append(row)
        if current_group:
            match_groups.append(current_group)

        next_p_col = rounds[r+1][0] if r+1 < len(rounds) else None
        df_next_players = df_bracket[(df_bracket['column'] == next_p_col) & (df_bracket['type'] == 'player')].sort_values('center_y') if next_p_col else pd.DataFrame()

        for i, group in enumerate(match_groups):
            avg_y = np.mean([row['center_y'] for row in group])

            if len(group) == 1:
                p1 = group[0]['text']
                p2 = 'BYE'
                score = 'N/A'
            elif len(group) == 2:
                p1 = group[0]['text']
                p2 = group[1]['text']
                if not df_scores.empty:
                    deltas = np.abs(df_scores['center_y'] - avg_y)
                    closest_idx = deltas.argmin()
                    score = df_scores.iloc[closest_idx]['text']
                    df_scores = df_scores.drop(df_scores.index[closest_idx])
                else:
                    score = 'N/A'
            else:
                continue

            p1_full, p1_club = player_club.get(p1, (p1, 'Unknown'))
            p2_full, p2_club = player_club.get(p2, (p2, 'Unknown')) if p2 != 'BYE' else ('BYE', '')
            p1_str = f"{p1_full} ({p1_club})"
            p2_str = f"{p2_full} ({p2_club})" if p2 != 'BYE' else 'BYE'

            if not df_next_players.empty:
                deltas = np.abs(df_next_players['center_y'] - avg_y)
                closest_idx = deltas.argmin()
                winner_short = df_next_players.iloc[closest_idx]['text']
                winner_full, winner_club = player_club.get(winner_short, (winner_short, 'Unknown'))
                winner = f"{winner_full} ({winner_club})"
                df_next_players = df_next_players.drop(df_next_players.index[closest_idx])
            else:
                winner = 'N/A'

            matches.append({
                'Round': round_names[r] if r < len(round_names) else f"Round {r+1}",
                'Player 1': p1_str,
                'Player 2': p2_str,
                'Winner': winner,
                'Score': score
            })

    df_matches = pd.DataFrame(matches)
    return df_matches.to_markdown(index=False)

# Example usage
print(parse_tournament_bracket("https://resultat.ondata.se/ViewClassPDF.php?classID=30021&stage=5"))