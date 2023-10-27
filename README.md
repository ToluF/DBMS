# Python-Scripted Database Management System

## Introduction
This Python-scripted database management system is designed to handle and execute SQL queries without relying on SQL or MySQL packages. It provides a lightweight, pure Python solution for managing data using a simple yet efficient approach.

## Features
- SQL-like query support.
- Data storage in plain text files.
- CRUD operations: Create, Read, Update, Delete.
- No external database server required.
- Minimal dependencies.

## Requirements
- Python 3.x (No external libraries required)

## Installation
1. Clone the repository to your local machine.
2. Navigate to the project directory.

## Usage
1. Define your database schema by creating a text file (e.g., "schema.txt") that outlines the structure of your tables.

2. Run the script to initialize the database:
   ```bash
   python initialize_db.py schema.txt
