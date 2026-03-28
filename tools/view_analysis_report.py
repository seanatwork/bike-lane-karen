#!/usr/bin/env python3
"""
User-friendly 311 Analysis Report Viewer

Provides interactive and formatted views of the global 311 analysis results.
"""

import json
import sys
from datetime import datetime
from typing import Dict, List, Any
from collections import defaultdict

class AnalysisReportViewer:
    def __init__(self, report_file: str = None):
        self.report_file = report_file or self.find_latest_report()
        self.report = self.load_report()
    
    def find_latest_report(self) -> str:
        """Find the most recent analysis report file"""
        import glob
        reports = glob.glob("311_analysis_report_*.json")
        if not reports:
            print("No analysis reports found. Run global_311_analysis.py first.")
            sys.exit(1)
        
        return sorted(reports)[-1]  # Return the most recent
    
    def load_report(self) -> Dict[str, Any]:
        """Load the JSON report file"""
        try:
            with open(self.report_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Report file not found: {self.report_file}")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Invalid JSON in report file: {self.report_file}")
            sys.exit(1)
    
    def print_executive_summary(self):
        """Print executive summary"""
        summary = self.report["summary"]
        
        print("\n" + "="*80)
        print("311 ANALYSIS EXECUTIVE SUMMARY")
        print("="*80)
        
        print(f"📊 Analysis Overview:")
        print(f"   • Categories analyzed: {summary['categories_analyzed']:,}")
        print(f"   • Tickets collected: {summary['total_tickets_collected']:,}")
        print(f"   • Recommendations generated: {summary['total_recommendations']:,}")
        
        print(f"\n🎯 Tool Opportunities:")
        for tool_type, count in sorted(summary["recommendation_types"].items(), 
                                      key=lambda x: x[1], reverse=True):
            icon = self.get_tool_icon(tool_type)
            print(f"   • {icon} {tool_type}: {count:,} opportunities")
    
    def get_tool_icon(self, tool_type: str) -> str:
        """Get emoji icon for tool type"""
        icons = {
            "Telegram bot": "🤖",
            "Real-time dashboard": "📈",
            "Geographic heatmap dashboard": "🗺️",
            "Maintenance request tracking dashboard with SLA monitoring": "🔧",
            "Temporal pattern analysis tool": "⏰"
        }
        return icons.get(tool_type, "📋")
    
    def print_top_opportunities(self, limit: int = 15):
        """Print top development opportunities"""
        print(f"\n" + "="*80)
        print(f"TOP {limit} DEVELOPMENT OPPORTUNITIES")
        print("="*80)
        
        for i, rec in enumerate(self.report["top_recommendations"][:limit], 1):
            category = rec["category"]
            recommendation = rec["recommendation"]
            volume = rec["category_volume"]
            tickets = rec["ticket_count"]
            
            print(f"\n{i:2d}. 🎯 {recommendation}")
            print(f"    📁 Category: {category}")
            print(f"    📊 Total volume: {volume:,} requests")
            print(f"    📋 Sample tickets: {tickets}")
    
    def print_category_deep_dive(self, category_name: str = None):
        """Print detailed analysis for specific category"""
        if not category_name:
            # Find highest volume category with data
            best_cat = None
            best_volume = 0
            for rec in self.report["top_recommendations"]:
                if rec["category_volume"] > best_volume:
                    best_volume = rec["category_volume"]
                    best_cat = rec["category"]
            category_name = best_cat
        
        if category_name not in self.report["detailed_analysis"]:
            print(f"No detailed analysis available for: {category_name}")
            return
        
        analysis = self.report["detailed_analysis"][category_name]
        
        print(f"\n" + "="*80)
        print(f"CATEGORY DEEP DIVE: {category_name.upper()}")
        print("="*80)
        
        print(f"📈 Sample Data Points: {analysis['total_tickets']}")
        
        # Pattern matches
        patterns = analysis.get("pattern_matches", {})
        if patterns:
            print(f"\n🔍 Pattern Analysis:")
            for pattern, count in patterns.items():
                if count > 0:
                    icon = self.get_pattern_icon(pattern)
                    print(f"   • {icon} {pattern.replace('_', ' ').title()}: {count} mentions")
        
        # Top keywords
        word_freq = analysis.get("word_frequency", {})
        if word_freq:
            print(f"\n📝 Top Keywords:")
            for word, count in list(word_freq.items())[:10]:
                print(f"   • {word}: {count} times")
        
        # Sample descriptions
        samples = analysis.get("sample_descriptions", [])
        if samples:
            print(f"\n💬 Sample Descriptions:")
            for i, desc in enumerate(samples[:3], 1):
                print(f"   {i}. \"{desc[:100]}{'...' if len(desc) > 100 else ''}\"")
    
    def get_pattern_icon(self, pattern: str) -> str:
        """Get emoji for pattern type"""
        icons = {
            "noise_words": "🔊",
            "parking_words": "🚗",
            "animal_words": "🐕",
            "maintenance_words": "🔧",
            "time_words": "⏰"
        }
        return icons.get(pattern, "📋")
    
    def print_tool_recommendations_by_type(self):
        """Group recommendations by tool type"""
        print(f"\n" + "="*80)
        print("RECOMMENDATIONS BY TOOL TYPE")
        print("="*80)
        
        # Group by tool type
        tool_groups = defaultdict(list)
        for rec in self.report["top_recommendations"]:
            tool_type = rec["recommendation"].split(" for ")[0]
            tool_groups[tool_type].append(rec)
        
        for tool_type, recs in sorted(tool_groups.items(), 
                                    key=lambda x: len(x[1]), reverse=True):
            icon = self.get_tool_icon(tool_type)
            print(f"\n{icon} {tool_type} ({len(recs)} opportunities)")
            print("-" * 60)
            
            for i, rec in enumerate(recs[:5], 1):  # Top 5 per category
                category = rec["category"]
                volume = rec["category_volume"]
                print(f"   {i}. {category} ({volume:,} total requests)")
            
            if len(recs) > 5:
                print(f"   ... and {len(recs) - 5} more")
    
    def print_high_volume_categories(self):
        """Focus on highest volume categories"""
        print(f"\n" + "="*80)
        print("HIGH-VOLUME CATEGORY ANALYSIS")
        print("="*80)
        
        # Get unique categories sorted by volume
        category_volumes = {}
        for rec in self.report["top_recommendations"]:
            category = rec["category"]
            category_volumes[category] = rec["category_volume"]
        
        sorted_cats = sorted(category_volumes.items(), key=lambda x: x[1], reverse=True)[:10]
        
        for i, (category, volume) in enumerate(sorted_cats, 1):
            print(f"\n{i:2d}. 📊 {category}")
            print(f"     Volume: {volume:,} total requests")
            
            # Find recommendations for this category
            cat_recs = [r for r in self.report["top_recommendations"] if r["category"] == category]
            for rec in cat_recs:
                tool_type = rec["recommendation"].split(" for ")[0]
                icon = self.get_tool_icon(tool_type)
                print(f"     • {icon} {tool_type}")
    
    def print_actionable_next_steps(self):
        """Print actionable next steps"""
        print(f"\n" + "="*80)
        print("ACTIONABLE NEXT STEPS")
        print("="*80)
        
        print(f"🚀 IMMEDIATE OPPORTUNITIES:")
        print(f"   1. 🤖 Build Telegram bot for noise complaints (highest impact)")
        print(f"   2. 🗺️ Create geographic heatmap for loose dog reports")
        print(f"   3. 📈 Real-time dashboard for graffiti tracking")
        
        print(f"\n📈 QUICK WINS:")
        print(f"   1. 🔧 Parking violation status bot (leverages existing code)")
        print(f"   2. ⏰ Temporal analysis for animal control patterns")
        print(f"   3. 📊 Maintenance SLA tracking dashboard")
        
        print(f"\n💡 TECHNICAL RECOMMENDATIONS:")
        print(f"   1. Use existing database structure for new tools")
        print(f"   2. Leverage Open311 API for real-time data")
        print(f"   3. Consider Telegram Bot API for citizen engagement")
        print(f"   4. Use mapping libraries for geographic visualizations")
    
    def interactive_menu(self):
        """Interactive menu for exploring the report"""
        while True:
            print(f"\n" + "="*80)
            print("311 ANALYSIS REPORT VIEWER")
            print("="*80)
            print(f"📁 Current report: {self.report_file}")
            print("\nOptions:")
            print("1. 📊 Executive Summary")
            print("2. 🎯 Top Opportunities")
            print("3. 🔍 Category Deep Dive")
            print("4. 🛠️ Recommendations by Tool Type")
            print("5. 📈 High-Volume Categories")
            print("6. 🚀 Actionable Next Steps")
            print("7. 📋 Full Report (Raw)")
            print("0. 🚪 Exit")
            
            choice = input("\nSelect option (0-7): ").strip()
            
            if choice == "0":
                print("Goodbye! 👋")
                break
            elif choice == "1":
                self.print_executive_summary()
            elif choice == "2":
                self.print_top_opportunities()
            elif choice == "3":
                cat = input("Enter category name (or press Enter for highest volume): ").strip()
                self.print_category_deep_dive(cat if cat else None)
            elif choice == "4":
                self.print_tool_recommendations_by_type()
            elif choice == "5":
                self.print_high_volume_categories()
            elif choice == "6":
                self.print_actionable_next_steps()
            elif choice == "7":
                print(json.dumps(self.report, indent=2, default=str))
            else:
                print("Invalid choice. Please try again.")
            
            input("\nPress Enter to continue...")


def main():
    """Main execution function"""
    viewer = AnalysisReportViewer()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--non-interactive":
        # Non-interactive mode - print everything
        viewer.print_executive_summary()
        viewer.print_top_opportunities()
        viewer.print_tool_recommendations_by_type()
        viewer.print_high_volume_categories()
        viewer.print_actionable_next_steps()
    else:
        # Interactive mode
        viewer.interactive_menu()


if __name__ == "__main__":
    main()
