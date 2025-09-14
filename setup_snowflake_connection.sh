#!/bin/bash

# Snowflake Connection Setup Script
# This will help you add your credentials to the connection file

echo "Setting up Snowflake connection..."
echo ""
echo "Your current connections.toml file is at: ~/.snowflake/connections.toml"
echo ""
echo "Please add your credentials to the file manually:"
echo ""
echo "1. Open Terminal and run: nano ~/.snowflake/connections.toml"
echo "2. Add your username and password to the [poc_connection] section"
echo "3. It should look like this:"
echo ""
echo '[poc_connection]'
echo 'accountname = "aja13247.us-east-1"'
echo 'username = "YOUR_USERNAME_HERE"'
echo 'password = "YOUR_PASSWORD_HERE"'
echo 'dbname = "POC"'
echo 'schemaname = "PUBLIC"'
echo 'warehousename = "X_SMALL_2_GEN"'
echo 'rolename = "ACCOUNTADMIN"'
echo ""
echo "After adding credentials, save the file (Ctrl+X, then Y, then Enter)"
echo ""
echo "Alternative quick method:"
echo "Replace YOUR_USERNAME_HERE and YOUR_PASSWORD_HERE in the command below:"
echo ""
echo 'sed -i.bak "s/YOUR_USERNAME_HERE/your_actual_username/g; s/YOUR_PASSWORD_HERE/your_actual_password/g" ~/.snowflake/connections.toml'
