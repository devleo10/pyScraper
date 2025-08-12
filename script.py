#!/usr/bin/env python3
"""
MF API Data Replication Script
Fetches all data from MF API and stores it in a local database
"""


import requests
import psycopg2
import psycopg2.extras
from psycopg2 import IntegrityError
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed output
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mfapi_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MFAPIReplicator:
    def __init__(self, db_url: str = None, base_url: str = "https://api.mfapi.in"):
        # Load environment variables from .env
        load_dotenv()
        self.db_url = db_url or os.getenv("DATABASE_URL")
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MF-API-Replicator/1.0',
            'Accept': 'application/json'
        })
        self.init_database()
    
    def get_conn(self):
        return psycopg2.connect(self.db_url, cursor_factory=psycopg2.extras.DictCursor)

    def init_database(self):
        """Initialize the database with required tables"""
        conn = self.get_conn()
        cursor = conn.cursor()

        # Mutual Fund Schemes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mf_schemes (
                scheme_code BIGINT PRIMARY KEY,
                scheme_name TEXT NOT NULL,
                scheme_category TEXT,
                scheme_type TEXT,
                fund_house TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # NAV Data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nav_data (
                id SERIAL PRIMARY KEY,
                scheme_code BIGINT,
                date TEXT,
                nav REAL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(scheme_code, date)
            )
        """)

        # Fund Details table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_details (
                scheme_code BIGINT PRIMARY KEY,
                fund_house TEXT,
                scheme_type TEXT,
                scheme_category TEXT,
                scheme_start_date TEXT,
                scheme_name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Historical NAV table for bulk storage
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_nav (
                id SERIAL PRIMARY KEY,
                scheme_code BIGINT,
                date TEXT,
                nav REAL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(scheme_code, date)
            )
        """)

        # Sync metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                id SERIAL PRIMARY KEY,
                table_name TEXT,
                last_sync TIMESTAMPTZ,
                records_count INTEGER,
                status TEXT
            )
        """)

        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_scheme_date ON nav_data(scheme_code, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_historical_nav_scheme_date ON historical_nav(scheme_code, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_scheme_name ON mf_schemes(scheme_name)")

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def make_request(self, endpoint: str, retries: int = 3) -> Dict[Any, Any]:
        """Make HTTP request with retry logic"""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def fetch_all_schemes(self) -> List[Dict]:
        """Fetch all mutual fund schemes"""
        logger.info("Fetching all MF schemes...")
        data = self.make_request("mf")
        schemes = data if isinstance(data, list) else []
        logger.info(f"Fetched {len(schemes)} schemes")
        return schemes
    
    def store_schemes(self, schemes: List[Dict]):
        """Store schemes in database with basic info only"""
        logger.info(f"Starting to store {len(schemes)} schemes...")
        conn = self.get_conn()
        cursor = conn.cursor()
        
        # Debug: Print first scheme to check data structure
        if schemes:
            logger.info(f"Sample scheme data: {schemes[0]}")
        
        batch_size = 1000
        stored_count = 0
        
        for i in range(0, len(schemes), batch_size):
            batch = schemes[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(schemes) + batch_size - 1) // batch_size
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} schemes)")
            
            for scheme in batch:
                try:
                    # Only store basic info from /mf endpoint
                    # Details will be fetched later from individual endpoints
                    cursor.execute("""
                        INSERT INTO mf_schemes 
                        (scheme_code, scheme_name, created_at, updated_at)
                        VALUES (%s, %s, NOW(), NOW())
                        ON CONFLICT (scheme_code) DO UPDATE SET 
                            scheme_name = EXCLUDED.scheme_name,
                            updated_at = NOW()
                    """, (
                        scheme.get('schemeCode'),
                        scheme.get('schemeName')
                    ))
                    stored_count += 1
                except Exception as e:
                    # Skip duplicates and other errors
                    logger.debug(f"Skipping scheme {scheme.get('schemeCode')}: {e}")
                    continue
            
            # Commit each batch
            conn.commit()
            logger.info(f"Committed batch {batch_num}, total stored: {stored_count}")
        
        conn.close()
        logger.info(f"Successfully stored {stored_count} schemes in database")
    
    def fetch_scheme_details(self, scheme_code: str) -> Dict:
        """Fetch detailed information for a specific scheme"""
        try:
            return self.make_request(f"mf/{scheme_code}")
        except Exception as e:
            logger.error(f"Error fetching details for scheme {scheme_code}: {e}")
            return {}
    
    def store_scheme_details(self, scheme_code: str, details: Dict):
        """Store detailed scheme information"""
        if not details:
            return
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            meta = details.get('meta', {})
            
            # Update the mf_schemes table with detailed information
            cursor.execute("""
                UPDATE mf_schemes SET 
                    fund_house = %s,
                    scheme_type = %s,
                    scheme_category = %s,
                    updated_at = NOW()
                WHERE scheme_code = %s
            """, (
                meta.get('fund_house', ''),
                meta.get('scheme_type', ''),
                meta.get('scheme_category', ''),
                scheme_code
            ))
            
            # Also store in fund_details table
            cursor.execute("""
                INSERT INTO fund_details 
                (scheme_code, fund_house, scheme_type, scheme_category, 
                 scheme_start_date, scheme_name, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (scheme_code) DO UPDATE SET 
                    fund_house=EXCLUDED.fund_house,
                    scheme_type=EXCLUDED.scheme_type,
                    scheme_category=EXCLUDED.scheme_category,
                    scheme_start_date=EXCLUDED.scheme_start_date,
                    scheme_name=EXCLUDED.scheme_name,
                    updated_at=NOW()
            """, (
                scheme_code,
                meta.get('fund_house', ''),
                meta.get('scheme_type', ''),
                meta.get('scheme_category', ''),
                meta.get('scheme_start_date', ''),
                meta.get('scheme_name', '')
            ))
            
            # Store NAV data
            nav_data = details.get('data', [])
            nav_stored = 0
            for nav_entry in nav_data:
                try:
                    cursor.execute("""
                        INSERT INTO historical_nav 
                        (scheme_code, date, nav)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (scheme_code, date) DO NOTHING
                    """, (
                        scheme_code,
                        nav_entry.get('date'),
                        float(nav_entry.get('nav', 0))
                    ))
                    nav_stored += 1
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid NAV data for scheme {scheme_code}: {e}")
            
            conn.commit()
            logger.debug(f"Stored details for scheme {scheme_code} with {nav_stored} NAV records")
        except Exception as e:
            logger.error(f"Error storing details for scheme {scheme_code}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def fetch_latest_nav(self) -> Dict:
        """Fetch latest NAV for all schemes"""
        logger.info("Fetching latest NAV data...")
        try:
            return self.make_request("mf")
        except Exception as e:
            logger.error(f"Error fetching latest NAV: {e}")
            return {}
    
    def store_latest_nav(self, nav_data: List[Dict]):
        """Store latest NAV data"""
        if not nav_data:
            return
        conn = self.get_conn()
        cursor = conn.cursor()
        today = datetime.now().strftime('%d-%m-%Y')
        
        stored_count = 0
        for nav_entry in nav_data:
            try:
                cursor.execute("""
                    INSERT INTO nav_data 
                    (scheme_code, date, nav)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (scheme_code, date) DO UPDATE SET nav=EXCLUDED.nav
                """, (
                    nav_entry.get('schemeCode'),
                    today,
                    float(nav_entry.get('nav', 0)) if nav_entry.get('nav') else None
                ))
                stored_count += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid NAV data: {e}")
            except Exception as e:
                logger.error(f"Error storing NAV data: {e}")
        conn.commit()
        conn.close()
        logger.info(f"Stored latest NAV for {stored_count} schemes")
    
    def sync_scheme_details_batch(self, scheme_codes: List[str], max_workers: int = 10):
        """Sync scheme details in batches using threading"""
        logger.info(f"Syncing details for {len(scheme_codes)} schemes...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_scheme = {
                executor.submit(self.sync_single_scheme, scheme_code): scheme_code 
                for scheme_code in scheme_codes
            }
            
            completed = 0
            for future in as_completed(future_to_scheme):
                scheme_code = future_to_scheme[future]
                try:
                    future.result()
                    completed += 1
                    if completed % 100 == 0:
                        logger.info(f"Completed {completed}/{len(scheme_codes)} schemes")
                except Exception as e:
                    logger.error(f"Error syncing scheme {scheme_code}: {e}")
                
                # Rate limiting
                time.sleep(0.1)
    
    def sync_single_scheme(self, scheme_code: str):
        """Sync a single scheme's details"""
        details = self.fetch_scheme_details(scheme_code)
        if details:
            self.store_scheme_details(scheme_code, details)
    
    def update_sync_metadata(self, table_name: str, records_count: int, status: str = "success"):
        """Update sync metadata"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO sync_metadata 
                (table_name, last_sync, records_count, status)
                VALUES (%s, NOW(), %s, %s)
            """, (table_name, records_count, status))
            conn.commit()
            logger.debug(f"Updated sync metadata for {table_name}")
        except Exception as e:
            logger.error(f"Error updating sync metadata: {e}")
        finally:
            conn.close()
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        conn = self.get_conn()
        cursor = conn.cursor()
        stats = {}
        tables = ['mf_schemes', 'nav_data', 'fund_details', 'historical_nav']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        conn.close()
        return stats
    
    def full_sync(self, include_historical: bool = True, max_workers: int = 10):
        """Perform a complete sync of all data"""
        start_time = datetime.now()
        logger.info("Starting full sync...")
        
        try:
            # Step 1: Fetch and store all schemes (basic info only)
            schemes = self.fetch_all_schemes()
            self.store_schemes(schemes)
            self.update_sync_metadata('mf_schemes', len(schemes))
            
            # Step 2: Store latest NAV
            self.store_latest_nav(schemes)
            self.update_sync_metadata('nav_data', len(schemes))
            
            # Step 3: Fetch detailed information for each scheme
            if include_historical:
                logger.info("Fetching detailed information for all schemes...")
                scheme_codes = [str(scheme['schemeCode']) for scheme in schemes if scheme.get('schemeCode')]
                
                # Process in smaller batches to avoid overwhelming the API
                batch_size = 100
                total_batches = (len(scheme_codes) + batch_size - 1) // batch_size
                
                for i in range(0, len(scheme_codes), batch_size):
                    batch = scheme_codes[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    logger.info(f"Processing details batch {batch_num}/{total_batches} ({len(batch)} schemes)")
                    
                    self.sync_scheme_details_batch(batch, max_workers)
                    
                    # Progress update
                    completed = min((i + batch_size), len(scheme_codes))
                    logger.info(f"Completed {completed}/{len(scheme_codes)} scheme details")
                
                self.update_sync_metadata('fund_details', len(scheme_codes))
            
            # Print statistics
            stats = self.get_database_stats()
            logger.info("Sync completed successfully!")
            logger.info("Database Statistics:")
            for table, count in stats.items():
                logger.info(f"  {table}: {count:,} records")
            
            duration = datetime.now() - start_time
            logger.info(f"Total sync time: {duration}")
            
        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise
    
    def detect_new_schemes(self, current_schemes: List[Dict]) -> List[Dict]:
        """Detect new schemes that are not in database"""
        conn = self.get_conn()
        cursor = conn.cursor()
        # Get existing scheme codes
        cursor.execute("SELECT scheme_code FROM mf_schemes")
        existing_codes = {row[0] for row in cursor.fetchall()}
        conn.close()
        # Find new schemes
        new_schemes = [
            scheme for scheme in current_schemes 
            if scheme.get('schemeCode') not in existing_codes
        ]
        logger.info(f"Found {len(new_schemes)} new mutual funds")
        return new_schemes
    
    def get_schemes_missing_details(self) -> List[str]:
        """Get scheme codes that don't have detailed information"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT scheme_code 
            FROM mf_schemes 
            WHERE fund_house IS NULL OR fund_house = '' 
               OR scheme_category IS NULL OR scheme_category = ''
               OR scheme_type IS NULL OR scheme_type = ''
        """)
        missing_schemes = [str(row[0]) for row in cursor.fetchall()]
        conn.close()
        logger.info(f"Found {len(missing_schemes)} schemes missing detailed information")
        return missing_schemes
    
    def get_schemes_needing_nav_update(self, days_back: int = 7) -> List[str]:
        """Get schemes that need NAV updates (haven't been updated recently)"""
        conn = self.get_conn()
        cursor = conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%d-%m-%Y')
        cursor.execute("""
            SELECT s.scheme_code 
            FROM mf_schemes s 
            LEFT JOIN (
                SELECT scheme_code, MAX(date) as last_nav_date 
                FROM historical_nav 
                GROUP BY scheme_code
            ) h ON s.scheme_code = h.scheme_code 
            WHERE h.last_nav_date IS NULL 
               OR h.last_nav_date < %s
        """, (cutoff_date,))
        
        outdated_schemes = [str(row[0]) for row in cursor.fetchall()]
        conn.close()
        
        logger.info(f"Found {len(outdated_schemes)} schemes needing NAV updates")
        return outdated_schemes
    
    def daily_sync(self, max_workers: int = 10):
        """Perform daily sync - detect new funds and update existing ones"""
        start_time = datetime.now()
        logger.info("Starting daily sync...")
        
        try:
            # Step 1: Fetch current scheme list
            current_schemes = self.fetch_all_schemes()
            
            # Step 2: Detect and add new schemes
            new_schemes = self.detect_new_schemes(current_schemes)
            if new_schemes:
                logger.info(f"Adding {len(new_schemes)} new mutual funds...")
                self.store_schemes(new_schemes)
                
                # Fetch details for new schemes
                new_scheme_codes = [str(scheme['schemeCode']) for scheme in new_schemes if scheme.get('schemeCode')]
                if new_scheme_codes:
                    self.sync_scheme_details_batch(new_scheme_codes, max_workers)
                    logger.info(f"Added detailed information for {len(new_scheme_codes)} new funds")
            
            # Step 3: Update latest NAV for all schemes
            logger.info("Updating latest NAV for all schemes...")
            self.store_latest_nav(current_schemes)
            
            # Step 4: Fill missing fund details (if any)
            missing_details = self.get_schemes_missing_details()
            if missing_details:
                logger.info(f"Fetching missing details for {len(missing_details)} schemes...")
                self.sync_scheme_details_batch(missing_details[:100], max_workers)  # Limit to 100 per day
            
            # Step 5: Update historical NAV for schemes that haven't been updated recently
            outdated_nav_schemes = self.get_schemes_needing_nav_update(days_back=3)
            if outdated_nav_schemes:
                logger.info(f"Updating historical NAV for {len(outdated_nav_schemes)} schemes...")
                # Limit to 50 schemes per day to avoid overwhelming the API
                self.sync_scheme_details_batch(outdated_nav_schemes[:50], max_workers)
            
            # Step 6: Update metadata
            self.update_sync_metadata('daily_sync', len(current_schemes))
            
            # Step 7: Log statistics
            stats = self.get_database_stats()
            logger.info("Daily sync completed successfully!")
            logger.info("Database Statistics:")
            for table, count in stats.items():
                logger.info(f"  {table}: {count:,} records")
            
            if new_schemes:
                logger.info(f"✓ Added {len(new_schemes)} new mutual funds")
            
            duration = datetime.now() - start_time
            logger.info(f"Daily sync time: {duration}")
            
            return {
                'new_schemes_added': len(new_schemes),
                'total_schemes': len(current_schemes),
                'missing_details_updated': min(len(missing_details), 100),
                'nav_updated_schemes': min(len(outdated_nav_schemes), 50),
                'duration': str(duration)
            }
            
        except Exception as e:
            logger.error(f"Daily sync failed: {e}")
            self.update_sync_metadata('daily_sync_error', 0, 'failed')
            raise
    
    def incremental_sync(self):
        """Perform incremental sync (latest NAV only)"""
        logger.info("Starting incremental sync...")
        
        try:
            schemes = self.fetch_all_schemes()
            
            # Detect new schemes
            new_schemes = self.detect_new_schemes(schemes)
            if new_schemes:
                logger.info(f"Found {len(new_schemes)} new schemes, adding them...")
                self.store_schemes(new_schemes)
            
            # Update NAV for all schemes
            self.store_latest_nav(schemes)
            self.update_sync_metadata('nav_data_incremental', len(schemes))
            
            logger.info(f"Incremental sync completed for {len(schemes)} schemes")
            if new_schemes:
                logger.info(f"Added {len(new_schemes)} new mutual funds")
            
        except Exception as e:
            logger.error(f"Incremental sync failed: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='MF API Data Replication Tool')
    parser.add_argument('--db-url', default=None, help='PostgreSQL connection string (overrides .env)')
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full', help='Sync mode: full or incremental')
    parser.add_argument('--no-historical', action='store_true', help='Skip historical data fetch in full sync')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker threads for parallel processing')

    args = parser.parse_args()

    # Initialize replicator, prefer CLI arg, else .env
    replicator = MFAPIReplicator(db_url=args.db_url)

    try:
        if args.mode == 'full':
            replicator.full_sync(
                include_historical=not args.no_historical,
                max_workers=args.workers
            )
        else:
            replicator.incremental_sync()

    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise

if __name__ == "__main__":
    main()