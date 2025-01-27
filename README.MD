# Contentful to PostgreSQL Migration Tool

This tool synchronizes data from Contentful to a PostgreSQL database. It handles subjects, topics, subtopics, questions, options, and quizzes while maintaining referential integrity.

## Prerequisites

1. Python 3.8 or higher
2. AWS Account access
3. Contentful account access

## Installation

1. Install Python:
   - Windows: Download and install from [python.org](https://www.python.org/downloads/)
   - Mac: `brew install python3`
   - Linux: `sudo apt install python3 python3-pip`

2. Create and activate a virtual environment: 
   - `python -m venv venv`
   - `source venv/bin/activate`

3. Install the required packages:
   - `pip install pg8000 contentful`

## Contentful Python Interpreter Setup

1. Select Python Interpreter:
   - Open Command Palette (Cmd/Ctrl + Shift + P)
   - Type "Python: Select Interpreter"
   - Choose the interpreter from your virtual environment:
     - It should be located at `./venv/bin/python` (Mac/Linux)
     - Or `./venv/Scripts/python.exe` (Windows)
     - Look for the one that shows "(venv)" next to it

## Configuration

1. Get AWS Credentials:
   - Log into [AWS Console](https://aws.amazon.com/console/)
   - Navigate to AWS Secrets Manager
   - Copy the DB_PASSWORD and DB_USER values
   - Copy the contentful access token

2. Get RDS Hostname:
   - In AWS Console, navigate to RDS
   - Find the BarTaker database instance you want to ingest to (encrypted is prod, unencrypted is dev)
   - Copy the endpoint (hostname)

3. Run the script:
    - use the play button in the top right of the file to run the script
        - if you get an error about the interpreter try running in debug mode, or try running in the terminal with `python migrate_to_postgres.py`. Make sure you have activated your virtual environment and completed the contentful python interpreter setup.
    - run the script in the terminal with `python migrate_to_postgres.py`
