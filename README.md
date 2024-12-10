# winddata

The Python pipeline cleans raw turbine data, calculates statistics, detects anomalies, and exports results. It handles daily updated CSV files for multiple turbines.

# Setup & Execution

* Install dependencies with: `pip install -r requirements.txt`
* Input: Place CSV files in a specified source directory (we haven't built a CLI so change the path in `main.py#l15` )
* Execution: Run `python main.py` from the project root.
* Output: CSV files saved in a specified destination directory – specified @ `main.py#l15`.

# How to Run Tests:
* `pip install -r requirements.txt` if you haven't
* From the project root directory, run: `pytest tests`


