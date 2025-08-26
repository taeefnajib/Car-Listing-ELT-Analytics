#!/bin/bash

# Step 1: Set environment variables
export FLASK_APP=superset
export SUPERSET_CONFIG_PATH=superset_config.py

echo "Environment variables set."

# Step 2: Upgrade the database
superset db upgrade
if [ $? -ne 0 ]; then
    echo "Database upgrade failed. Exiting."
    exit 1
fi

echo "Database upgraded successfully."

# Step 3: Create admin user
read -p "Enter username: " USERNAME
read -p "Enter email: " EMAIL
read -p "Enter first name: " FIRST_NAME
read -p "Enter last name: " LAST_NAME
read -s -p "Enter password: " PASSWORD
echo
read -s -p "Confirm password: " CONFIRM_PASSWORD

if [ "$PASSWORD" != "$CONFIRM_PASSWORD" ]; then
    echo "Passwords do not match. Exiting."
    exit 1
fi

superset fab create-admin \
    --username "$USERNAME" \
    --firstname "$FIRST_NAME" \
    --lastname "$LAST_NAME" \
    --email "$EMAIL" \
    --password "$PASSWORD"

if [ $? -ne 0 ]; then
    echo "Admin creation failed. Exiting."
    exit 1
fi

echo "Admin user created successfully."

# Step 4: Initialize Superset
superset init
if [ $? -ne 0 ]; then
    echo "Initialization failed. Exiting."
    exit 1
fi

echo "Superset initialized successfully."

# Step 5: Run Superset
superset run -p 8088 --with-threads --reload 