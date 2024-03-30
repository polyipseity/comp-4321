# Installation instructions

**Step 1:**
Set up a Python environment: Ensure that you have Python installed on your system. You can download the latest version of Python from the official Python website (<https://www.python.org>) and follow the installation instructions for your operating system.

_Note: For Windows, you may want to install the Python launcher, enabling you to use `py` in place of `python` for consistently running the latest version of Python, avoiding any conflicts with third-party software and outdated Python versions._

_**After doing so, replace all instances of `python` with `py` in the following commands.**_

**Step 2:**
Clone the repository: Open a terminal or command prompt and navigate to the directory where you want to clone the repository. Then, run the following command to clone the repository:

```shell
git clone https://github.com/polyipseity/comp-4321.git
```

This will create a new directory named "comp-4321" and clone the repository into it.

_You may also download zip, but then you need to take care when navigating to the project directory in step 3._

**Step 3:**
Navigate to the project directory: Use the cd command to navigate to the project directory. Run the following command:

```shell
cd comp-4321
```

**Step 4:**
**Create a virtual environment (optional but highly recommended): Given how other teams may also use Python, and the dependencies used between projects may have conflicts, it is highly recommended to create a virtual environment for running our project.** To create a virtual environment, run the following command:

```shell
python -m venv venv
```

This command creates a new virtual environment named "venv" in the "comp-4321" directory.

The virtual environment can effectively avoid issues such as:
_ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
fastapi 0.104.1 requires anyio<4.0.0,>=3.7.1, but you have anyio 4.3.0 which is incompatible._

**Step 5:**
Activate the virtual environment: Activate the virtual environment using the appropriate command based on your operating system:

On Windows:

```shell
venv\Scripts\activate
```

On Linux or macOS:

```shell
source venv/bin/activate
```

**Step 6:**
Install the required packages: In the root directory of the project (i.e., the "comp-4321" directory), there should be a file named "requirements.txt". To install the required packages, run the following command:

_Note: Check again to see if `(venv)` appears in the command prompt for using the virtual environment._

```shell
pip install -r requirements.txt
```

This command will install all the necessary packages specified in the "requirements.txt" file.

**Step 7:**
Run the crawler using the command for Phase 1.

_Note: Check again to see if `(venv)` appears in the command prompt for using the virtual environment._

```shell
python -m egod_search.crawl -n 30 -d database.db -s spider_result.txt https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm
```

In case of re-run, and the database needs to be cleared, use the appropriate command based on your operating system:

On Windows:

```shell
del database.db
```

On Linux or macOS:

```shell
rm database.db
```
