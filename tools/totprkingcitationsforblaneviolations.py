import sqlite3
import re


def extract_citation_count(status_notes):
    """Extract citation count from status_notes text"""
    if not status_notes:
        return 1
    
    status_notes = status_notes.lower()
    
    # Handle written numbers first
    word_numbers = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10
    }
    
    for word, num in word_numbers.items():
        if f'{word} citation' in status_notes:
            return num
    
    # Extract numbers before "citation"
    patterns = [
        r'(\d+)\s+citations?\s+issued',
        r'(\d+)\s+citation',
        r'citations?\s+issued[:\.]?\s*(\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, status_notes)
        if match:
            return int(match.group(1))
    
    # Default to 1 if no number found
    return 1


def analyze_citations(db_path="311_categories.db"):
    """Analyze citation counts from Open311 data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT status_notes 
    FROM open311_requests 
    WHERE status_notes LIKE '%Cite Vehicle(s)%'
      AND (lower(status_notes) LIKE '%citation%issued%' 
           OR lower(status_notes) LIKE '%citations%issued%')
      AND lower(status_notes) NOT LIKE '%warn%'
      AND attributes_json LIKE '%"code"%BIKEL007%'
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    total_citations = 0
    examples = []
    citation_counts = {}
    
    for (status_notes,) in rows:
        count = extract_citation_count(status_notes)
        total_citations += count
        
        # Track distribution of citation counts
        citation_counts[count] = citation_counts.get(count, 0) + 1
        
        # Collect some examples for verification
        if len(examples) < 15:
            examples.append((status_notes, count))
    
    print(f"Total records: {len(rows)}")
    print(f"Total citations: {total_citations}")
    print(f"Average citations per record: {total_citations / len(rows):.2f}")
    
    print("\nCitation count distribution:")
    for count in sorted(citation_counts.keys()):
        print(f"  {count} citations: {citation_counts[count]} records")
    
    print("\nSample extractions:")
    for notes, count in examples:
        print(f"  '{notes}' → {count}")
    
    conn.close()
    return total_citations


if __name__ == "__main__":
    analyze_citations()
