-- Car Listing Database Initialization Script
-- This runs when the container starts for the first time

-- Create schemas for organizing data
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS analytics;