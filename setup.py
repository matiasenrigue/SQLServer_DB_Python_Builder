from setuptools import setup, find_packages

with open("requirements.txt") as f:
    content = f.readlines()
requirements = [x.strip() for x in content]

setup(
    name = "SQLServer_DB_Builder",
    description="A Python package to build SQL Server databases and SSIS packages",
    packages=find_packages(),
    install_requires=requirements
    )
