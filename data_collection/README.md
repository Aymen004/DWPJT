# Google Maps Bank Reviews Scraper for Morocco

This module contains a scraper that extracts customer reviews for bank agencies in Morocco from Google Maps.

## Features

- Searches for bank agencies in major Moroccan cities
- Extracts detailed information about each agency
- Collects customer reviews including text, rating, and date
- Detects review language automatically
- Exports data to JSON or CSV format
- Includes logging for debugging and monitoring

## Requirements

- Python 3.8+
- Chrome browser
- ChromeDriver (must match your Chrome version)
- Dependencies listed in requirements.txt

## Installation

1. Install required Python packages:

```bash
pip install -r ../requirements.txt
```

2. Make sure you have Chrome browser installed

3. Download ChromeDriver that matches your Chrome version from:
   https://sites.google.com/chromium.org/driver/downloads

## Usage

Run the script from command line with the following arguments:

```bash

python data_collection/google_maps_scraper.py --output data_collection/reviews.json --banks "Attijariwafa Bank,CIH Bank,Banque Poupulaire,BMCE Bank of Africa,Al Barid Bank, Ccredit Agricole du Maroc" --cities "Casablanca,Rabat,Marrakech,Agadir,Tangier,Tetouan,Fes,Meknes" 
```

### Command-line Arguments

- `--output`: Output file path (either .json or .csv)
- `--banks`: Comma-separated list of bank names to search for
- `--cities`: Comma-separated list of Moroccan cities to search in (default: Casablanca,Rabat,Marrakech,Tangier,Fes)
- `--headless`: Run browser in headless mode (optional)
- `--max_reviews`: Maximum number of reviews to collect per agency (default: 20)


```

## Output Format

The script generates either JSON or CSV files with the following fields:

- `agency_name`: Name of the bank agency
- `bank`: Name of the bank
- `location`: Full address location
- `city`: City name
- `text`: Review text content
- `rating`: Star rating (1-5)
- `date`: Estimated date of review (YYYY-MM-DD)
- `language`: Detected language of the review
- `url`: Google Maps URL for the agency

## Troubleshooting

If you encounter any issues:

1. Check the generated `google_maps_scraper.log` file for detailed logs
2. Make sure your ChromeDriver version matches your Chrome browser version
3. Try running without `--headless` to see what's happening in the browser
4. Check if Google Maps structure has changed (selectors may need updates)

## Notes

- The script includes random delays between requests to avoid rate limiting
- Limited to 10 agencies per city-bank combination to avoid long execution times
- Reviews dates are approximated from relative dates shown on Google Maps