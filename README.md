# User Status Processor

Script for processing and updating user status based on termination files.

## Description

This script processes two CSV files:
1. A current users export
2. A terminations file

And generates a new CSV file containing only the users that need to be marked as inactive.

## Requirements

- Python 3.8 or higher
- pandas

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd user-status-processor
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Place the input files in the project directory:
   - `NinjoEmployees export.csv`: Current users file
   - `YYYYMMDD_81OP_Terminations_All_Countries_Clara 1.csv`: Terminations file

2. Run the script:
```bash
python user_status_processor.py
```

The script will generate an output file with the format:
`YYYYMMDD_users_to_inactivate.csv`

## Validations

The script includes the following validations:
- No duplicates in the output
- Only includes users who were active
- Verifies that users are in the terminations list
- Validates the presence of all required fields
- Checks for null values in critical fields

## Project Structure

```
user-status-processor/
├── README.md
├── requirements.txt
├── user_status_processor.py
└── .gitignore
```

## Contributing

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Authors

* **Your Name** - *Initial work*

## Acknowledgments

* Thanks to the Clara team for the requirements and support
* Special thanks to all contributors
