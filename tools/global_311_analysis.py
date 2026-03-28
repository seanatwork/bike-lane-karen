#!/usr/bin/env python3
"""
Global 311 Analysis Script

Pulls 5 most recent tickets from each 311 category via Open311 API,
analyzes descriptions, and recommends Telegram bot or dashboard opportunities.
"""

import json
import sqlite3
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, Counter
import requests

# Import functions from existing open311_ingest.py
from open311_ingest import (
    _requests_session, 
    _request_json_with_backoff,
    _ensure_schema,
    _upsert_requests,
    _isoformat_z,
    _utc_now,
    OPEN311_BASE_URL
)

class Global311Analyzer:
    def __init__(self, db_path: str = "311_categories.db"):
        self.db_path = db_path
        self.session = None
        self.categories = {}
        self.analysis_data = []
        
    def load_categories_from_file(self, filename: str = "311categories.txt") -> Dict[str, int]:
        """Parse 311categories.txt to extract category names and counts"""
        categories = {}
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and '(' in line and ')' in line:
                        # Format: "Category Name (count)" with commas in numbers
                        match = re.match(r'^(.+?) \(([\d,]+)\)$', line)
                        if match:
                            name = match.group(1).strip()
                            count_str = match.group(2).replace(',', '')  # Remove commas
                            count = int(count_str)
                            categories[name] = count
        except FileNotFoundError:
            print(f"Error: {filename} not found")
            return {}
        
        print(f"Loaded {len(categories)} categories from file")
        return categories
    
    def discover_service_code(self, category_name: str) -> Optional[str]:
        """Discover service code for a category using Open311 search"""
        url = f"{OPEN311_BASE_URL}/requests.json"
        params = {
            "q": category_name,
            "per_page": 10,
            "page": 1
        }
        
        try:
            payload = _request_json_with_backoff(self.session, url, params)
            if isinstance(payload, list) and len(payload) > 0:
                # Look for exact matches first
                for request in payload:
                    service_name = request.get("service_name", "")
                    if service_name.lower() == category_name.lower():
                        return request.get("service_code")
                
                # Fall back to first result if no exact match
                return payload[0].get("service_code")
        except Exception as e:
            print(f"Error discovering service code for {category_name}: {e}")
        
        return None
    
    def get_recent_tickets(self, service_code: str, limit: int = 5) -> List[Dict]:
        """Get most recent tickets for a service code"""
        url = f"{OPEN311_BASE_URL}/requests.json"
        params = {
            "service_code": service_code,
            "per_page": limit,
            "page": 1,
            "extensions": "true"
        }
        
        try:
            payload = _request_json_with_backoff(self.session, url, params)
            if isinstance(payload, list):
                return payload
        except Exception as e:
            print(f"Error getting tickets for {service_code}: {e}")
        
        return []
    
    def analyze_text_patterns(self, tickets: List[Dict]) -> Dict[str, Any]:
        """Analyze text patterns in ticket descriptions"""
        if not tickets:
            return {}
        
        descriptions = []
        locations = []
        status_notes = []
        
        for ticket in tickets:
            desc = ticket.get("description", "")
            if desc:
                descriptions.append(desc.lower())
            
            loc = ticket.get("address", "")
            if loc:
                locations.append(loc.lower())
            
            notes = ticket.get("status_notes", "")
            if notes:
                status_notes.append(notes.lower())
        
        # Extract common keywords
        all_text = " ".join(descriptions + status_notes)
        words = re.findall(r'\b\w+\b', all_text)
        word_freq = Counter(words)
        
        # Common patterns
        patterns = {
            "noise_words": ["loud", "noise", "music", "party", "construction"],
            "parking_words": ["parking", "vehicle", "car", "ticket", "citation"],
            "animal_words": ["dog", "animal", "loose", "barking", "bite"],
            "maintenance_words": ["pothole", "repair", "broken", "damage", "street"],
            "time_words": ["night", "morning", "evening", "early", "late"]
        }
        
        pattern_matches = {}
        for pattern_name, pattern_words in patterns.items():
            matches = sum(1 for word in pattern_words if word in all_text)
            pattern_matches[pattern_name] = matches
        
        return {
            "total_tickets": len(tickets),
            "word_frequency": dict(word_freq.most_common(20)),
            "pattern_matches": pattern_matches,
            "has_locations": len(locations) > 0,
            "sample_descriptions": descriptions[:3]
        }
    
    def generate_recommendations(self, category_name: str, analysis: Dict[str, Any]) -> List[str]:
        """Generate tool recommendations based on analysis"""
        recommendations = []
        
        if not analysis:
            return recommendations
        
        pattern_matches = analysis.get("pattern_matches", {})
        total_tickets = analysis.get("total_tickets", 0)
        
        # Telegram Bot recommendations
        if pattern_matches.get("noise_words", 0) > 0:
            recommendations.append("Telegram bot for noise complaint tracking and escalation")
        
        if pattern_matches.get("parking_words", 0) > 0:
            recommendations.append("Telegram bot for parking violation reporting and status updates")
        
        if pattern_matches.get("animal_words", 0) > 0:
            recommendations.append("Telegram bot for animal control alerts and status tracking")
        
        # Dashboard recommendations
        if total_tickets >= 3:
            recommendations.append(f"Real-time dashboard for {category_name} monitoring")
        
        if analysis.get("has_locations", False):
            recommendations.append("Geographic heatmap dashboard for issue clustering")
        
        if pattern_matches.get("maintenance_words", 0) > 0:
            recommendations.append("Maintenance request tracking dashboard with SLA monitoring")
        
        # Data analysis recommendations
        if pattern_matches.get("time_words", 0) > 0:
            recommendations.append("Temporal pattern analysis tool for peak time identification")
        
        return recommendations
    
    def run_analysis(self) -> Dict[str, Any]:
        """Run complete analysis pipeline"""
        print("Starting global 311 analysis...")
        
        # Initialize database connection
        conn = sqlite3.connect(self.db_path)
        _ensure_schema(conn)
        
        # Initialize API session
        self.session = _requests_session(None)
        
        # Load categories
        self.categories = self.load_categories_from_file()
        
        all_recommendations = []
        category_analysis = {}
        total_tickets_collected = 0
        
        print(f"Analyzing {len(self.categories)} categories...")
        
        for i, (category_name, category_count) in enumerate(self.categories.items(), 1):
            print(f"[{i}/{len(self.categories)}] Processing: {category_name}")
            
            # Discover service code
            service_code = self.discover_service_code(category_name)
            if not service_code:
                print(f"  No service code found for {category_name}")
                continue
            
            # Get recent tickets
            tickets = self.get_recent_tickets(service_code, 5)
            if not tickets:
                print(f"  No tickets found for {category_name}")
                continue
            
            # Store tickets in database
            _upsert_requests(conn, tickets)
            total_tickets_collected += len(tickets)
            
            # Analyze patterns
            analysis = self.analyze_text_patterns(tickets)
            category_analysis[category_name] = analysis
            
            # Generate recommendations
            recommendations = self.generate_recommendations(category_name, analysis)
            for rec in recommendations:
                all_recommendations.append({
                    "category": category_name,
                    "recommendation": rec,
                    "ticket_count": len(tickets),
                    "category_volume": category_count
                })
            
            # Rate limiting
            time.sleep(1)
        
        conn.close()
        
        # Generate summary report
        report = self.generate_report(all_recommendations, category_analysis, total_tickets_collected)
        
        print(f"\nAnalysis complete!")
        print(f"Categories processed: {len(category_analysis)}")
        print(f"Tickets collected: {total_tickets_collected}")
        print(f"Recommendations generated: {len(all_recommendations)}")
        
        return report
    
    def generate_report(self, recommendations: List[Dict], analysis: Dict[str, Any], total_tickets: int) -> Dict[str, Any]:
        """Generate comprehensive analysis report"""
        
        # Count recommendation types
        rec_types = defaultdict(int)
        for rec in recommendations:
            rec_type = rec["recommendation"].split(" for ")[0]  # Extract tool type
            rec_types[rec_type] += 1
        
        # Sort recommendations by category volume
        sorted_recs = sorted(recommendations, key=lambda x: x["category_volume"], reverse=True)
        
        # Top categories by pattern matches
        top_noise = sorted(
            [(cat, data.get("pattern_matches", {}).get("noise_words", 0)) 
             for cat, data in analysis.items()], 
            key=lambda x: x[1], reverse=True
        )[:5]
        
        top_parking = sorted(
            [(cat, data.get("pattern_matches", {}).get("parking_words", 0)) 
             for cat, data in analysis.items()], 
            key=lambda x: x[1], reverse=True
        )[:5]
        
        report = {
            "summary": {
                "categories_analyzed": len(analysis),
                "total_tickets_collected": total_tickets,
                "total_recommendations": len(recommendations),
                "recommendation_types": dict(rec_types)
            },
            "top_recommendations": sorted_recs[:10],
            "top_noise_categories": top_noise,
            "top_parking_categories": top_parking,
            "detailed_analysis": analysis
        }
        
        # Save report to file
        report_file = f"311_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"Report saved to: {report_file}")
        
        return report
    
    def print_summary(self, report: Dict[str, Any]):
        """Print analysis summary to console"""
        summary = report["summary"]
        
        print("\n" + "="*60)
        print("GLOBAL 311 ANALYSIS REPORT")
        print("="*60)
        
        print(f"\nSUMMARY:")
        print(f"Categories analyzed: {summary['categories_analyzed']}")
        print(f"Tickets collected: {summary['total_tickets_collected']}")
        print(f"Recommendations generated: {summary['total_recommendations']}")
        
        print(f"\nRECOMMENDATION TYPES:")
        for rec_type, count in summary["recommendation_types"].items():
            print(f"  {rec_type}: {count}")
        
        print(f"\nTOP 10 RECOMMENDATIONS:")
        for i, rec in enumerate(report["top_recommendations"], 1):
            print(f"{i:2d}. {rec['recommendation']} ({rec['category']})")
        
        print(f"\nTOP NOISE-RELATED CATEGORIES:")
        for cat, count in report["top_noise_categories"]:
            if count > 0:
                print(f"  {cat}: {count} noise indicators")
        
        print(f"\nTOP PARKING-RELATED CATEGORIES:")
        for cat, count in report["top_parking_categories"]:
            if count > 0:
                print(f"  {cat}: {count} parking indicators")


def main():
    """Main execution function"""
    analyzer = Global311Analyzer()
    
    try:
        report = analyzer.run_analysis()
        analyzer.print_summary(report)
        
        print(f"\nNext steps:")
        print(f"1. Review the detailed JSON report for specific recommendations")
        print(f"2. Prioritize high-volume categories for tool development")
        print(f"3. Consider API integration for real-time data")
        
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
