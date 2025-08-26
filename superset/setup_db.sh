#!/bin/bash

# Create SQL commands in a temporary file
cat << EOF > /tmp/setup_db.sql
-- First create the user/role
CREATE USER admin WITH PASSWORD 'admin';
-- Then create the database
CREATE DATABASE superset;
-- Finally grant privileges
ALTER DATABASE superset OWNER TO superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
EOF

# Execute the SQL commands
sudo -u postgres psql -f /tmp/setup_db.sql

# Clean up the temporary file
rm /tmp/setup_db.sql 