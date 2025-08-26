#!/usr/bin/env python3
"""
Database management script for car listing project
"""

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import sys

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres'
}

def get_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def test_connection():
    """Test database connection"""
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print(f"✅ Database connection successful!")
                print(f"PostgreSQL version: {version}")
                
                # Check if our schemas exist
                cur.execute("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('raw', 'staging', 'analytics')
                    ORDER BY schema_name;
                """)
                schemas = [row[0] for row in cur.fetchall()]
                print(f"✅ Schemas found: {schemas}")
                
                # Check if our tables exist
                cur.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_schema IN ('raw', 'staging')
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()
                print("✅ Tables found:")
                for schema, table in tables:
                    print(f"  - {schema}.{table}")
                    
        except Exception as e:
            print(f"❌ Error testing database: {e}")
        finally:
            conn.close()
    else:
        print("❌ Could not connect to database")

def run_summary_report():
    """Run the summary report function"""
    conn = get_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM analytics.generate_summary_report();")
                results = cur.fetchall()
                
                print("\n📊 Database Summary Report:")
                print("=" * 40)
                for row in results:
                    print(f"{row['metric']}: {row['value']}")
                    
        except Exception as e:
            print(f"❌ Error running summary report: {e}")
        finally:
            conn.close()

def view_sample_data(table_name="raw.car_listings", limit=5):
    """View sample data from a table"""
    conn = get_connection()
    if conn:
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit};"
            df = pd.read_sql_query(query, conn)
            print(f"\n📋 Sample data from {table_name}:")
            print("=" * 50)
            print(df.to_string(index=False))
            
        except Exception as e:
            print(f"❌ Error viewing sample data: {e}")
        finally:
            conn.close()

def transform_data():
    """Run the data transformation function"""
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT staging.transform_car_listings();")
                result = cur.fetchone()[0]
                conn.commit()
                print(f"✅ Transformation completed. {result} records processed.")
                
        except Exception as e:
            print(f"❌ Error running transformation: {e}")
            conn.rollback()
        finally:
            conn.close()

def view_analytics():
    """View analytics views"""
    conn = get_connection()
    if conn:
        try:
            # Brand summary
            print("\n📈 Brand Summary (Top 10):")
            print("=" * 50)
            df_brands = pd.read_sql_query("""
                SELECT * FROM analytics.brand_summary 
                WHERE total_listings > 0
                LIMIT 10;
            """, conn)
            print(df_brands.to_string(index=False))
            
            # Monthly listings
            print("\n📅 Monthly Listings:")
            print("=" * 30)
            df_monthly = pd.read_sql_query("""
                SELECT * FROM analytics.monthly_listings 
                LIMIT 10;
            """, conn)
            print(df_monthly.to_string(index=False))
            
        except Exception as e:
            print(f"❌ Error viewing analytics: {e}")
        finally:
            conn.close()

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("""
🚗 Car Listing Database Management Tool

Usage: python db_management.py <command>

Commands:
  test        - Test database connection and show schema info
  summary     - Show summary report
  sample      - View sample raw data
  transform   - Run data transformation from raw to staging
  analytics   - View analytics reports
  
Examples:
  python db_management.py test
  python db_management.py sample
  python db_management.py transform
        """)
        return
    
    command = sys.argv[1].lower()
    
    if command == "test":
        test_connection()
    elif command == "summary":
        run_summary_report()
    elif command == "sample":
        view_sample_data()
    elif command == "transform":
        transform_data()
    elif command == "analytics":
        view_analytics()
    else:
        print(f"❌ Unknown command: {command}")

if __name__ == "__main__":
    main()