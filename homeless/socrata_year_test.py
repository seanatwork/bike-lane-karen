#!/usr/bin/env python3
"""
Test Socrata query for 1 year of data to estimate time and volume.
"""

import os
import time
import logging
import requests
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.austintexas.gov/resource"
DATASET_ID = "xwdj-i9he"


def test_year_query():
    """Query Socrata for 1 year of homeless-relevant service requests."""
    logger.info("=" * 70)
    logger.info("SOCRATA YEAR QUERY TEST")
    logger.info("=" * 70)
    
    # Calculate date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=365)
    
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    
    logger.info(f"\nDate range: {start_str} to {end_str}")
    logger.info(f"Querying Socrata for 365 days of data...")
    
    # Build service type filter (same as hybrid test)
    type_filters = [
        "contains(upper(sr_type_desc), 'PARK')",
        "contains(upper(sr_type_desc), 'DEBRIS')",
        "contains(upper(sr_type_desc), 'DRAINAGE')",
        "contains(upper(sr_type_desc), 'CHANNEL')",
        "contains(upper(sr_type_desc), 'RIGHT OF WAY')",
        "contains(upper(sr_type_desc), 'OBSTRUCTION')",
    ]
    
    # Socrata has a 50,000 record limit per query
    # For 365 days, we might need pagination
    all_records = []
    offset = 0
    limit = 50000  # Socrata max
    batch = 0
    
    session = requests.Session()
    
    while True:
        batch += 1
        params = {
            "$where": f"sr_created_date >= '{start_str}' AND sr_created_date <= '{end_str}' AND ({' OR '.join(type_filters)})",
            "$select": "sr_number,sr_created_date,sr_status_desc,sr_location,sr_location_lat,sr_location_long,sr_type_desc",
            "$limit": limit,
            "$offset": offset,
            "$order": "sr_created_date DESC",
        }
        
        url = f"{SOCRATA_BASE}/{DATASET_ID}.json"
        
        logger.info(f"\nBatch {batch}: Fetching records {offset} to {offset + limit}...")
        start_time = time.time()
        
        try:
            resp = session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            records = resp.json()
            fetch_time = time.time() - start_time
            
            logger.info(f"  ✓ Fetched {len(records)} records in {fetch_time:.1f}s")
            
            if not records:
                logger.info("  No more records")
                break
            
            all_records.extend(records)
            
            if len(records) < limit:
                logger.info("  Last batch reached")
                break
            
            offset += limit
            
            # Safety check - if we somehow hit more than 200k records, stop
            if offset > 200000:
                logger.warning("  Hit safety limit of 200k records, stopping")
                break
            
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            break
    
    total_time = time.time() - start_time
    
    logger.info("\n" + "=" * 70)
    logger.info("RESULTS")
    logger.info("=" * 70)
    logger.info(f"Total records fetched: {len(all_records)}")
    logger.info(f"Total batches: {batch}")
    logger.info(f"Total query time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    
    if all_records:
        # Analyze data
        date_range = datetime.fromisoformat(all_records[0]['sr_created_date'].replace('Z', '+00:00')), datetime.fromisoformat(all_records[-1]['sr_created_date'].replace('Z', '+00:00'))
        logger.info(f"\nData range: {date_range[1].strftime('%Y-%m-%d')} to {date_range[0].strftime('%Y-%m-%d')}")
        
        # Count by type
        type_counts = {}
        for r in all_records:
            t = r.get('sr_type_desc', 'Unknown')
            type_counts[t] = type_counts.get(t, 0) + 1
        
        logger.info("\nTop 10 service types:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  {c:5d} - {t}")
        
        # Estimate daily volume
        days_covered = (date_range[0] - date_range[1]).days
        daily_avg = len(all_records) / max(days_covered, 1)
        logger.info(f"\nDaily average: {daily_avg:.1f} records/day")
    
    logger.info("\n" + "=" * 70)
    logger.info("CACHING RECOMMENDATION")
    logger.info("=" * 70)
    
    if len(all_records) > 50000:
        logger.info("⚠️ Large dataset detected (>50k records)")
        logger.info("   Recommendation: CACHE the Socrata query results")
        logger.info("   - Query once per day and store in SQLite/JSON")
        logger.info("   - Use cached data for filtering/mapping")
        logger.info("   - Only fetch new records incrementally")
    elif len(all_records) > 10000:
        logger.info("📊 Medium dataset (~10k-50k records)")
        logger.info("   Recommendation: Optional caching")
        logger.info("   - Query time is manageable (<2 min)")
        logger.info("   - But caching would speed up repeated queries")
    else:
        logger.info("✅ Small dataset (<10k records)")
        logger.info("   Recommendation: No caching needed")
        logger.info("   - Query is fast enough to run on-demand")
    
    logger.info("\n" + "=" * 70)
    
    return {
        "total_records": len(all_records),
        "query_time": total_time,
        "batches": batch,
        "daily_avg": len(all_records) / 365 if all_records else 0,
    }


if __name__ == "__main__":
    test_year_query()
