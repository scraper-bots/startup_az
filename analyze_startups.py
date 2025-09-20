"""
Startup Azerbaijan Data Analysis
Analyzes startup data from CSV and generates visualization charts
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter
import re
import os

# Set style for better-looking charts
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Create assets directory if it doesn't exist
os.makedirs('assets', exist_ok=True)

def load_and_clean_data():
    """Load and clean the startup data"""
    df = pd.read_csv('startup_az_linkwise.csv')

    # Remove duplicate/empty rows
    df = df.dropna(subset=['listing_title'])
    df = df[df['listing_title'].str.strip() != '']

    # Clean segment data
    df['Segment'] = df['Segment'].fillna('Unknown')
    df['Segment'] = df['Segment'].str.strip()

    # Clean status data
    df['Status'] = df['Status'].fillna('Unknown')
    df['Status'] = df['Status'].str.strip()

    # Clean investment data
    df['Investments'] = df['Investments'].fillna('No data')
    df['Investments'] = df['Investments'].str.strip()

    return df

def analyze_segments(df):
    """Analyze startup segments"""
    # Count startups by segment
    segment_counts = df['Segment'].value_counts()

    # Group smaller segments into "Others" for better readability
    top_segments = segment_counts.head(7)  # Show top 7 segments
    others_count = segment_counts[7:].sum() if len(segment_counts) > 7 else 0

    if others_count > 0:
        plot_data = pd.concat([top_segments, pd.Series([others_count], index=['Others'])])
    else:
        plot_data = top_segments

    # Create a more readable pie chart
    plt.figure(figsize=(14, 10))
    colors = plt.cm.Set3(np.linspace(0, 1, len(plot_data)))

    # Create pie chart with better formatting
    wedges, texts, autotexts = plt.pie(plot_data.values,
                                      labels=None,  # Remove labels from pie
                                      autopct='%1.1f%%',
                                      colors=colors,
                                      startangle=90,
                                      pctdistance=0.85)

    # Create legend with counts
    legend_labels = [f'{label}: {count} startups' for label, count in plot_data.items()]
    plt.legend(wedges, legend_labels,
              title="Business Segments",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1),
              fontsize=11)

    plt.title('Distribution of Startups by Business Segment',
              fontsize=18, fontweight='bold', pad=20)
    plt.axis('equal')

    # Make percentage text more readable
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontsize(10)
        autotext.set_fontweight('bold')

    plt.tight_layout()
    plt.savefig('assets/segments_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    return segment_counts

def analyze_status(df):
    """Analyze startup status"""
    status_counts = df['Status'].value_counts()

    # Create bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(range(len(status_counts)), status_counts.values,
                   color=plt.cm.viridis(np.linspace(0, 1, len(status_counts))))

    plt.title('Startup Status Distribution', fontsize=16, fontweight='bold')
    plt.xlabel('Status', fontsize=12)
    plt.ylabel('Number of Startups', fontsize=12)
    plt.xticks(range(len(status_counts)), status_counts.index, rotation=45, ha='right')

    # Add value labels on bars
    for bar, value in zip(bars, status_counts.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                str(value), ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('assets/status_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

    return status_counts

def analyze_investments(df):
    """Analyze investment patterns"""
    # Clean and categorize investment data
    investment_categories = []

    for inv in df['Investments']:
        if pd.isna(inv) or inv in ['Axtarılmır', 'No data', '----']:
            investment_categories.append('Not seeking')
        elif '25 min' in str(inv):
            investment_categories.append('Up to 25k AZN')
        elif '50 min' in str(inv):
            investment_categories.append('Up to 50k AZN')
        else:
            investment_categories.append('Other/Unknown')

    df['Investment_Category'] = investment_categories
    inv_counts = pd.Series(investment_categories).value_counts()

    # Create horizontal bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.barh(range(len(inv_counts)), inv_counts.values,
                    color=plt.cm.plasma(np.linspace(0, 1, len(inv_counts))))

    plt.title('Investment Seeking Patterns', fontsize=16, fontweight='bold')
    plt.xlabel('Number of Startups', fontsize=12)
    plt.ylabel('Investment Category', fontsize=12)
    plt.yticks(range(len(inv_counts)), inv_counts.index)

    # Add value labels
    for bar, value in zip(bars, inv_counts.values):
        plt.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                str(value), ha='left', va='center', fontweight='bold')

    plt.tight_layout()
    plt.savefig('assets/investment_patterns.png', dpi=300, bbox_inches='tight')
    plt.close()

    return inv_counts

def analyze_segment_vs_status(df):
    """Cross-analysis of segments vs status"""
    # Create crosstab
    crosstab = pd.crosstab(df['Segment'], df['Status'])

    # Create heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(crosstab, annot=True, fmt='d', cmap='YlOrRd',
                cbar_kws={'label': 'Number of Startups'})

    plt.title('Startup Status by Business Segment', fontsize=16, fontweight='bold')
    plt.xlabel('Status', fontsize=12)
    plt.ylabel('Business Segment', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig('assets/segment_status_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()

    return crosstab

def analyze_team_size(df):
    """Analyze team sizes where available"""
    team_sizes = []

    for team in df['Team']:
        if pd.isna(team) or team in ['----', '---']:
            continue

        # Extract numbers from team description
        numbers = re.findall(r'\d+', str(team))
        if numbers:
            team_sizes.append(int(numbers[0]))

    if team_sizes:
        plt.figure(figsize=(10, 6))
        plt.hist(team_sizes, bins=range(1, max(team_sizes)+2), alpha=0.7,
                color='skyblue', edgecolor='black')

        plt.title('Team Size Distribution', fontsize=16, fontweight='bold')
        plt.xlabel('Team Size', fontsize=12)
        plt.ylabel('Number of Startups', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('assets/team_size_distribution.png', dpi=300, bbox_inches='tight')
        plt.close()

    return team_sizes

def generate_summary_stats(df):
    """Generate summary statistics"""
    stats = {
        'total_startups': len(df),
        'total_segments': df['Segment'].nunique(),
        'most_common_segment': df['Segment'].mode().iloc[0],
        'most_common_status': df['Status'].mode().iloc[0],
        'startups_with_websites': df['Website'].notna().sum(),
        'startups_with_emails': df['Email'].notna().sum(),
        'startups_with_certification': df['Certification'].notna().sum()
    }

    return stats

def main():
    """Main analysis function"""
    print("Loading and cleaning data...")
    df = load_and_clean_data()

    print(f"Analyzing {len(df)} startups...")

    # Generate analyses
    print("Analyzing segments...")
    segment_counts = analyze_segments(df)

    print("Analyzing status...")
    status_counts = analyze_status(df)

    print("Analyzing investments...")
    investment_counts = analyze_investments(df)

    print("Analyzing segment vs status...")
    crosstab = analyze_segment_vs_status(df)

    print("Analyzing team sizes...")
    team_sizes = analyze_team_size(df)

    print("Generating summary statistics...")
    stats = generate_summary_stats(df)

    print("\n=== ANALYSIS COMPLETE ===")
    print(f"Total startups analyzed: {stats['total_startups']}")
    print(f"Charts saved to assets/ folder")

    return {
        'stats': stats,
        'segment_counts': segment_counts,
        'status_counts': status_counts,
        'investment_counts': investment_counts,
        'crosstab': crosstab,
        'team_sizes': team_sizes
    }

if __name__ == "__main__":
    results = main()