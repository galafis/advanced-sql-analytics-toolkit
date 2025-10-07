from setuptools import setup, find_packages

setup(
    name="advanced-sql-analytics-toolkit",
    version="1.0.0",
    author="Gabriel Demetrios Lafis",
    author_email="gabriel@example.com",
    description="Professional Advanced Sql Analytics Toolkit framework",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/galafis/advanced-sql-analytics-toolkit",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "numpy>=1.24.0",
        "pandas>=2.0.0",
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
    ],
)
