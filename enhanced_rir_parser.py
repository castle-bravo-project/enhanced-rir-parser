#!/usr/bin/env python3
"""
RIR Data Parser - Creates IP-to-Country lookup table from Regional Internet Registry data
Enhanced version with error handling, progress tracking, optimization, and JSON export
"""

import requests
import ipaddress
import sqlite3
import csv
import json
from datetime import datetime
import gzip
import io
import time
import sys
from typing import Dict, List, Optional, Tuple

class RIRDataParser:
    def __init__(self, db_path="ip_country.db"):
        self.db_path = db_path
        self.rir_urls = {
            'ARIN': 'https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest',
            'RIPE': 'https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest',
            'APNIC': 'https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest',
            'LACNIC': 'https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest',
            'AFRINIC': 'https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest'
        }
        
    def setup_database(self):
        """Create SQLite database and tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table for IPv4 ranges
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ipv4_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ip INTEGER,
                end_ip INTEGER,
                country_code TEXT,
                rir TEXT,
                date_allocated TEXT,
                status TEXT
            )
        ''')
        
        # Create table for IPv6 ranges
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ipv6_ranges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                network TEXT,
                prefix_length INTEGER,
                country_code TEXT,
                rir TEXT,
                date_allocated TEXT,
                status TEXT
            )
        ''')
        
        # Create table for metadata
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipv4_start ON ipv4_ranges(start_ip)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipv4_end ON ipv4_ranges(end_ip)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipv4_country ON ipv4_ranges(country_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipv6_network ON ipv6_ranges(network)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ipv6_country ON ipv6_ranges(country_code)')
        
        conn.commit()
        conn.close()
        
    def ip_to_int(self, ip_str: str) -> int:
        """Convert IP address string to integer"""
        return int(ipaddress.IPv4Address(ip_str))
    
    def cidr_to_range(self, network_str: str, prefix_length: int) -> Tuple[int, int]:
        """Convert CIDR notation to start/end IP integers"""
        network = ipaddress.IPv4Network(f"{network_str}/{prefix_length}", strict=False)
        return int(network.network_address), int(network.broadcast_address)
    
    def download_rir_data(self, rir_name: str) -> Optional[str]:
        """Download RIR data file with retry logic"""
        url = self.rir_urls[rir_name]
        print(f"Downloading {rir_name} data from {url}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=120, stream=True)
                response.raise_for_status()
                
                # Check if content is gzipped
                content_encoding = response.headers.get('content-encoding', '')
                if content_encoding == 'gzip':
                    return gzip.decompress(response.content).decode('utf-8')
                else:
                    return response.text
                    
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {rir_name}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)  # Wait before retry
                else:
                    print(f"Failed to download {rir_name} data after {max_retries} attempts")
                    return None
    
    def parse_rir_line(self, line: str) -> Optional[Dict]:
        """Parse a single line from RIR data file"""
        if line.startswith('#') or not line.strip():
            return None
            
        fields = line.strip().split('|')
        if len(fields) < 7:
            return None
            
        registry, cc, type_field, start, value, date, status = fields[:7]
        
        # Skip non-country entries
        if cc == '*' or len(cc) != 2:
            return None
            
        # We only want IP allocations
        if type_field not in ['ipv4', 'ipv6']:
            return None
            
        # Validate date format
        if date and len(date) == 8:
            try:
                datetime.strptime(date, '%Y%m%d')
            except ValueError:
                date = ''
        
        return {
            'registry': registry,
            'country_code': cc.upper(),
            'type': type_field,
            'start': start,
            'value': value,
            'date': date,
            'status': status
        }
    
    def process_rir_data(self, data: str, rir_name: str) -> List[Dict]:
        """Process RIR data and return parsed entries"""
        entries = []
        lines = data.split('\n')
        
        print(f"Processing {len(lines)} lines from {rir_name}")
        
        for i, line in enumerate(lines):
            if i % 10000 == 0 and i > 0:
                print(f"  Processed {i:,} lines...")
                
            entry = self.parse_rir_line(line)
            if entry:
                entry['rir'] = rir_name
                entries.append(entry)
                
        return entries
    
    def insert_entries_to_db(self, entries: List[Dict]):
        """Insert parsed entries into database with batch processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        ipv4_entries = []
        ipv6_entries = []
        
        print(f"Processing {len(entries)} entries for database insertion...")
        
        for i, entry in enumerate(entries):
            if i % 5000 == 0 and i > 0:
                print(f"  Prepared {i:,} entries...")
            
            if entry['type'] == 'ipv4':
                try:
                    # For IPv4, 'value' is the number of IPs
                    num_ips = int(entry['value'])
                    start_ip = self.ip_to_int(entry['start'])
                    end_ip = start_ip + num_ips - 1
                    
                    ipv4_entries.append((
                        start_ip, end_ip, entry['country_code'],
                        entry['rir'], entry['date'], entry['status']
                    ))
                except (ValueError, ipaddress.AddressValueError) as e:
                    print(f"Error processing IPv4 entry: {entry}, Error: {e}")
                    continue
                    
            elif entry['type'] == 'ipv6':
                try:
                    # For IPv6, 'value' is the prefix length
                    prefix_length = int(entry['value'])
                    
                    ipv6_entries.append((
                        entry['start'], prefix_length, entry['country_code'],
                        entry['rir'], entry['date'], entry['status']
                    ))
                except ValueError as e:
                    print(f"Error processing IPv6 entry: {entry}, Error: {e}")
                    continue
        
        # Insert IPv4 entries in batches
        if ipv4_entries:
            print(f"Inserting {len(ipv4_entries):,} IPv4 entries...")
            cursor.executemany('''
                INSERT INTO ipv4_ranges (start_ip, end_ip, country_code, rir, date_allocated, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ipv4_entries)
            print(f"✓ Inserted {len(ipv4_entries):,} IPv4 entries")
        
        # Insert IPv6 entries in batches
        if ipv6_entries:
            print(f"Inserting {len(ipv6_entries):,} IPv6 entries...")
            cursor.executemany('''
                INSERT INTO ipv6_ranges (network, prefix_length, country_code, rir, date_allocated, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ipv6_entries)
            print(f"✓ Inserted {len(ipv6_entries):,} IPv6 entries")
        
        conn.commit()
        conn.close()
    
    def update_metadata(self, key: str, value: str):
        """Update metadata table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (key, value))
        
        conn.commit()
        conn.close()
    
    def build_table(self):
        """Main function to build the complete IP-to-country table"""
        print("=" * 60)
        print("RIR Data Parser - Building IP-to-Country Database")
        print("=" * 60)
        
        start_time = time.time()
        
        print("\n1. Setting up database...")
        self.setup_database()
        
        print("2. Clearing existing data...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM ipv4_ranges')
        cursor.execute('DELETE FROM ipv6_ranges')
        conn.commit()
        conn.close()
        
        # Download and process data from each RIR
        total_entries = 0
        successful_rirs = []
        
        for rir_name in self.rir_urls.keys():
            print(f"\n3. Processing {rir_name}...")
            print("-" * 30)
            
            data = self.download_rir_data(rir_name)
            
            if data:
                entries = self.process_rir_data(data, rir_name)
                if entries:
                    self.insert_entries_to_db(entries)
                    total_entries += len(entries)
                    successful_rirs.append(rir_name)
                    print(f"✓ Successfully processed {len(entries):,} entries from {rir_name}")
                else:
                    print(f"✗ No valid entries found in {rir_name} data")
            else:
                print(f"✗ Failed to download {rir_name} data")
        
        # Update metadata
        self.update_metadata('last_updated', datetime.now().isoformat())
        self.update_metadata('total_entries', str(total_entries))
        self.update_metadata('successful_rirs', ','.join(successful_rirs))
        
        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print(f"✓ Database build completed in {elapsed_time:.1f} seconds")
        print(f"✓ Total entries processed: {total_entries:,}")
        print(f"✓ Successful RIRs: {', '.join(successful_rirs)}")
        print(f"✓ Database created: {self.db_path}")
        
        # Print summary statistics
        self.print_summary()
    
    def print_summary(self):
        """Print database summary statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM ipv4_ranges')
        ipv4_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ipv6_ranges')
        ipv6_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT country_code, COUNT(*) FROM ipv4_ranges GROUP BY country_code ORDER BY COUNT(*) DESC LIMIT 10')
        top_countries = cursor.fetchall()
        
        cursor.execute('SELECT rir, COUNT(*) FROM ipv4_ranges GROUP BY rir ORDER BY COUNT(*) DESC')
        rir_stats = cursor.fetchall()
        
        print(f"\n" + "=" * 60)
        print("DATABASE SUMMARY")
        print("=" * 60)
        print(f"IPv4 ranges: {ipv4_count:,}")
        print(f"IPv6 ranges: {ipv6_count:,}")
        print(f"Total ranges: {ipv4_count + ipv6_count:,}")
        
        print(f"\nTop 10 countries by IPv4 allocations:")
        for cc, count in top_countries:
            print(f"  {cc}: {count:,}")
        
        print(f"\nRIR distribution:")
        for rir, count in rir_stats:
            print(f"  {rir}: {count:,}")
        
        conn.close()
    
    def lookup_ip(self, ip_address: str) -> Optional[Dict]:
        """Lookup country for a given IP address"""
        try:
            ip_obj = ipaddress.ip_address(ip_address)
            
            if ip_obj.version == 4:
                return self.lookup_ipv4(str(ip_obj))
            else:
                return self.lookup_ipv6(str(ip_obj))
                
        except ValueError:
            return None
    
    def lookup_ipv4(self, ip_address: str) -> Optional[Dict]:
        """Lookup IPv4 address in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        ip_int = self.ip_to_int(ip_address)
        
        cursor.execute('''
            SELECT country_code, rir, date_allocated, status 
            FROM ipv4_ranges 
            WHERE start_ip <= ? AND end_ip >= ?
            ORDER BY end_ip - start_ip
            LIMIT 1
        ''', (ip_int, ip_int))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'ip_address': ip_address,
                'country_code': result[0],
                'rir': result[1],
                'date_allocated': result[2],
                'status': result[3],
                'ip_version': 4
            }
        return None
    
    def lookup_ipv6(self, ip_address: str) -> Optional[Dict]:
        """Lookup IPv6 address in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT network, prefix_length, country_code, rir, date_allocated, status FROM ipv6_ranges')
        
        ip_obj = ipaddress.IPv6Address(ip_address)
        
        for row in cursor.fetchall():
            network_str, prefix_length, cc, rir, date_allocated, status = row
            try:
                network = ipaddress.IPv6Network(f"{network_str}/{prefix_length}", strict=False)
                if ip_obj in network:
                    conn.close()
                    return {
                        'ip_address': ip_address,
                        'country_code': cc,
                        'rir': rir,
                        'date_allocated': date_allocated,
                        'status': status,
                        'ip_version': 6,
                        'network': str(network)
                    }
            except ValueError:
                continue
                
        conn.close()
        return None
    
    def bulk_lookup(self, ip_list: List[str]) -> List[Dict]:
        """Perform bulk IP lookups"""
        results = []
        print(f"Performing bulk lookup for {len(ip_list)} IP addresses...")
        
        for i, ip in enumerate(ip_list):
            if i % 1000 == 0 and i > 0:
                print(f"  Processed {i:,} IPs...")
            
            result = self.lookup_ip(ip)
            if result:
                results.append(result)
            else:
                results.append({
                    'ip_address': ip,
                    'country_code': None,
                    'rir': None,
                    'date_allocated': None,
                    'status': None,
                    'ip_version': None
                })
        
        return results
    
    def export_to_csv(self, output_file: str = "ip_country_table.csv"):
        """Export IPv4 ranges to CSV for web app use"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print(f"Exporting IPv4 data to {output_file}...")
        
        cursor.execute('''
            SELECT start_ip, end_ip, country_code, rir, date_allocated, status 
            FROM ipv4_ranges 
            ORDER BY start_ip
        ''')
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['start_ip', 'end_ip', 'country_code', 'rir', 'date_allocated', 'status'])
            
            row_count = 0
            for row in cursor.fetchall():
                writer.writerow(row)
                row_count += 1
                
                if row_count % 50000 == 0:
                    print(f"  Exported {row_count:,} rows...")
        
        conn.close()
        print(f"✓ IPv4 data exported to {output_file} ({row_count:,} rows)")
    
    def export_to_json(self, output_file: str = "ipv4_ranges.json"):
        """Export IPv4 ranges to JSON in the exact format required by the other app"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        print(f"Exporting IPv4 data to JSON format: {output_file}...")
        
        cursor.execute('''
            SELECT start_ip, end_ip, country_code 
            FROM ipv4_ranges 
            ORDER BY start_ip
        ''')
        
        json_data = []
        row_count = 0
        
        for row in cursor.fetchall():
            start_ip, end_ip, country_code = row
            
            # Create the exact structure required
            json_entry = {
                "start": start_ip,
                "end": end_ip,
                "country": country_code
            }
            
            json_data.append(json_entry)
            row_count += 1
            
            if row_count % 50000 == 0:
                print(f"  Processed {row_count:,} rows...")
        
        conn.close()
        
        # Write to JSON file with proper formatting
        print(f"Writing {len(json_data):,} entries to {output_file}...")
        
        with open(output_file, 'w', encoding='utf-8') as jsonfile:
            json.dump(json_data, jsonfile, indent=2, separators=(',', ': '))
        
        print(f"✓ IPv4 data exported to {output_file} ({row_count:,} entries)")
        print(f"✓ File size: {self._get_file_size(output_file)}")
        
        # Show a sample of the exported data
        print(f"\nSample of exported JSON data:")
        print(json.dumps(json_data[:3], indent=2))
        if len(json_data) > 3:
            print("  ... (truncated)")
    
    def _get_file_size(self, filepath: str) -> str:
        """Get human-readable file size"""
        import os
        size = os.path.getsize(filepath)
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    
    def export_country_stats(self, output_file: str = "country_stats.csv"):
        """Export country statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                country_code,
                COUNT(*) as allocation_count,
                SUM(end_ip - start_ip + 1) as total_ips,
                rir,
                MIN(date_allocated) as first_allocation,
                MAX(date_allocated) as last_allocation
            FROM ipv4_ranges 
            GROUP BY country_code, rir
            ORDER BY total_ips DESC
        ''')
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['country_code', 'allocation_count', 'total_ips', 'rir', 'first_allocation', 'last_allocation'])
            
            for row in cursor.fetchall():
                writer.writerow(row)
        
        conn.close()
        print(f"✓ Country statistics exported to {output_file}")

# CLI Interface
def main():
    """Main CLI interface"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python rir_parser.py build              # Build database")
        print("  python rir_parser.py lookup <ip>        # Lookup single IP")
        print("  python rir_parser.py export [filename]  # Export to CSV")
        print("  python rir_parser.py json [filename]    # Export to JSON")
        print("  python rir_parser.py stats [filename]   # Export country stats")
        sys.exit(1)
    
    parser = RIRDataParser()
    command = sys.argv[1].lower()
    
    if command == 'build':
        parser.build_table()
        
    elif command == 'lookup':
        if len(sys.argv) < 3:
            print("Please provide an IP address to lookup")
            sys.exit(1)
        
        ip = sys.argv[2]
        result = parser.lookup_ip(ip)
        
        if result:
            print(f"IP: {result['ip_address']}")
            print(f"Country: {result['country_code']}")
            print(f"RIR: {result['rir']}")
            print(f"Date Allocated: {result['date_allocated']}")
            print(f"Status: {result['status']}")
            print(f"IP Version: {result['ip_version']}")
        else:
            print(f"No information found for {ip}")
            
    elif command == 'export':
        filename = sys.argv[2] if len(sys.argv) > 2 else "ip_country_table.csv"
        parser.export_to_csv(filename)
        
    elif command == 'json':
        filename = sys.argv[2] if len(sys.argv) > 2 else "ipv4_ranges.json"
        parser.export_to_json(filename)
        
    elif command == 'stats':
        filename = sys.argv[2] if len(sys.argv) > 2 else "country_stats.csv"
        parser.export_country_stats(filename)
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

# Example usage
if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        # Default behavior - build and test
        parser = RIRDataParser()
        
        # Build the complete table
        parser.build_table()
        
        # Test some lookups
        test_ips = [
            "8.8.8.8",          # Google DNS
            "1.1.1.1",          # Cloudflare DNS
            "208.67.222.222",   # OpenDNS
            "192.168.1.1",      # Private IP
            "134.195.196.26",   # Academic IP
            "23.185.0.2",       # Akamai
            "2001:4860:4860::8888"  # Google IPv6 DNS
        ]
        
        print("\n" + "=" * 60)
        print("TESTING IP LOOKUPS")
        print("=" * 60)
        
        for ip in test_ips:
            result = parser.lookup_ip(ip)
            if result:
                print(f"{ip:<20} → {result['country_code']} ({result['rir']})")
            else:
                print(f"{ip:<20} → Not found")
        
        # Export to CSV for web app
        parser.export_to_csv()
        parser.export_country_stats()
        
        # Export to JSON in the exact format required
        parser.export_to_json()