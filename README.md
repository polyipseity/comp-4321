# Installation instructions

## Step 1

Set up a Python environment: Ensure that you have at least Python >= 3.11 installed on your system. You can download the latest version of Python from the official Python website (<https://www.python.org>) and follow the installation instructions for your operating system.

_Note: For Windows, you may want to install the Python launcher, enabling you to use `py` in place of `python` for consistently running the latest version of Python, avoiding any conflicts with third-party software and outdated Python versions._

_**After doing so, replace all instances of `python` with `py` in the following commands.**_

## Step 2

Unzip the submission file and navigate to the extracted folder.

Then, open a terminal at the folder.

## Step 3

**Create a virtual environment (highly recommended): Given how other teams may also use Python, and the dependencies used between projects may have conflicts, it is highly recommended to create a virtual environment for running our project.** To create a virtual environment, run the following command:

```shell
python -m venv venv
```

This command creates a new virtual environment named "venv" in the "comp-4321" directory.

The virtual environment can effectively avoid issues such as:
_ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
fastapi 0.104.1 requires anyio<4.0.0,>=3.7.1, but you have anyio 4.3.0 which is incompatible._

## Step 4

Activate the virtual environment: Activate the virtual environment using the appropriate command based on your operating system:

On Windows:

```shell
venv\Scripts\activate
```

On Linux or macOS:

```shell
source venv/bin/activate
```

## Step 5

Install the required packages: In the root directory of the project (i.e., the "comp-4321" directory), there should be a file named "requirements.txt". To install the required packages, run the following command:

_Note: Check again to see if `(venv)` appears in the command prompt for using the virtual environment._

```shell
pip install -r requirements.txt
```

This command will install all the necessary packages specified in the "requirements.txt" file.

## Step 6

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

## Important notices

The program says it is `Finished` but does not end, just gets stuck:
On Windows, after the program has finished, the CLI may freeze if the program finishes too quickly. This is a [CPython bug](https://github.com/python/cpython/issues/111604) and is out of our control. Just Ctrl+C to get out of it and ignore the errors as they are harmless.  

If there is an error mentioning `requires a different Python`, for example `ERROR: Package 'egod-search' requires a different Python: 3.10.11 not in '>=3.11.0'`:
Your Python version is outdated and does not support [features the code relies on](https://stackoverflow.com/a/77247460). Please go to <https://www.python.org/downloads/> and download the newest version of Python.

## FAQ

Q: Install does not work

A1: Check again that `(venv)` appears in the command prompt for using the virtual environment. The virtual environment is not entered by default.

Q: `venv\Scripts\activate` does not work for my Windows machine

A: For Windows machines with MinGW-w64, `python.exe` may refer to the MinGW-w64 executable. It does not work because it generates Linux version of virtual environment script, and likely does not come with Python >= 3.11. Use `py` which can guarantee running the Windows Python executable.

## Tested working platforms

Linux: Debian 12 on Python 3.11.2 (older Linux distros do not have >= Python 3.11, either install yourself or switch machines)
Windows: Windows 10 and 11, Python 3.11.2 and 3.12.2
