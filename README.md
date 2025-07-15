# RIR Data Parser 🌍
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](link) ![Static Badge](https://img.shields.io/badge/Validation-Pending-red) [![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A Python tool that downloads and processes Regional Internet Registry (RIR) data to create IP-to-country lookup tables. Supports multiple output formats including JSON, CSV, and SQLite database.

## Short Description

This script downloads IP allocation data from all five Regional Internet Registries (ARIN, RIPE, APNIC, LACNIC, AFRINIC) and creates a comprehensive database mapping IP address ranges to countries. Perfect for geolocation services, network analysis, and security applications.

## Features

- **Multi-RIR Support**: Downloads from all 5 Regional Internet Registries
- **Multiple Output Formats**: JSON, CSV, SQLite database
- **IPv4 & IPv6 Support**: Handles both IP versions
- **Progress Tracking**: Real-time progress updates during processing
- **Error Handling**: Robust retry logic for network operations
- **Optimized Performance**: Batch processing and database indexing
- **CLI Interface**: Easy-to-use command-line interface

## Requirements

- Python 3.6+
- `requests` library

## Installation

1. Clone or download the script
2. Install the required dependency:
   ```bash
   pip install requests
   ```

## Quick Start

```bash
# Build the complete database
python rir_parser.py build

# Export to JSON format (perfect for web apps)
python rir_parser.py json ipv4_ranges.json

# Look up a specific IP address
python rir_parser.py lookup 8.8.8.8
```

## Usage

### Building the Database
```bash
python rir_parser.py build
```
Downloads data from all RIRs and creates a SQLite database (`ip_country.db`).

### Exporting Data

#### JSON Format (Recommended for Web Apps)
```bash
python rir_parser.py json [filename]
```
Creates a JSON file with the structure:
```json
[
  {
    "start": 16843009,
    "end": 16843009,
    "country": "US"
  }
]
```

#### CSV Format
```bash
python rir_parser.py export [filename]
```
Creates a CSV file with columns: `start_ip`, `end_ip`, `country_code`, `rir`, `date_allocated`, `status`.

#### Country Statistics
```bash
python rir_parser.py stats [filename]
```
Exports country-level statistics including IP allocation counts and date ranges.

### IP Lookup
```bash
python rir_parser.py lookup <ip_address>
```
Look up country information for a specific IP address.

## Output Formats

### JSON Export
The JSON export creates an array of objects with exactly three keys:
- `start`: Starting IP address as integer
- `end`: Ending IP address as integer  
- `country`: Two-letter ISO 3166-1 alpha-2 country code

This format is optimized for web applications and can be directly used in JavaScript/TypeScript projects.

### CSV Export
The CSV export includes additional metadata:
- `start_ip`: Starting IP address as integer
- `end_ip`: Ending IP address as integer
- `country_code`: Two-letter country code
- `rir`: Regional Internet Registry name
- `date_allocated`: Allocation date (YYYYMMDD format)
- `status`: Allocation status

## Database Schema

The SQLite database contains three tables:

### `ipv4_ranges`
- `start_ip`: INTEGER (IP as integer)
- `end_ip`: INTEGER (IP as integer)  
- `country_code`: TEXT (2-letter code)
- `rir`: TEXT (Registry name)
- `date_allocated`: TEXT (YYYYMMDD)
- `status`: TEXT (Allocation status)

### `ipv6_ranges`
- `network`: TEXT (IPv6 network)
- `prefix_length`: INTEGER
- `country_code`: TEXT
- `rir`: TEXT
- `date_allocated`: TEXT
- `status`: TEXT

### `metadata`
- `key`: TEXT (Metadata key)
- `value`: TEXT (Metadata value)
- `updated_at`: TIMESTAMP

## Data Sources

The script downloads data from these Regional Internet Registries:

- **ARIN** (North America): `ftp.arin.net`
- **RIPE** (Europe/Middle East): `ftp.ripe.net`
- **APNIC** (Asia-Pacific): `ftp.apnic.net`
- **LACNIC** (Latin America): `ftp.lacnic.net`
- **AFRINIC** (Africa): `ftp.afrinic.net`

## Examples

### Basic Usage
```bash
# Build database and export to JSON
python rir_parser.py build
python rir_parser.py json my_ranges.json

# Look up some IPs
python rir_parser.py lookup 8.8.8.8        # Google DNS
python rir_parser.py lookup 1.1.1.1        # Cloudflare DNS
python rir_parser.py lookup 208.67.222.222 # OpenDNS
```

### Integration with Web Apps
```bash
# Export optimized JSON for web applications
python rir_parser.py json ipv4_ranges.json

# Use in JavaScript/TypeScript
const ranges = require('./ipv4_ranges.json');
```

## Performance Notes

- Initial database build takes 2-5 minutes depending on internet speed
- Processes 400,000+ IP ranges from all RIRs
- SQLite database is ~50-100MB
- JSON export is ~30-60MB
- IP lookups are optimized with database indexes

## Error Handling

The script includes comprehensive error handling:
- Automatic retries for network failures
- Graceful handling of malformed data
- Progress tracking with detailed logging
- Validation of IP addresses and date formats

## Contributing

Feel free to submit issues and pull requests. Areas for improvement:
- Additional output formats
- Database optimization
- IPv6 lookup performance
- API server mode

## License

Open source - use as needed for your projects.

## Changelog

### Latest Version
- ✅ Added JSON export functionality
- ✅ Enhanced CLI interface
- ✅ Improved error handling
- ✅ Added progress tracking
- ✅ Optimized database performance
- ✅ Added country statistics export
