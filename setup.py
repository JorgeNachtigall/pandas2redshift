from setuptools import find_packages, setup
import pathlib

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name="pandas2redshift",
    version="0.0.3",
    author="Jorge Nachtigall",
    author_email="jlnvjunior@gmail.com",
    description="A tool for exporting Pandas dataframes to Redshift tables",
    long_description=README,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=["boto3>=1.34.109", "SQLAlchemy>=1.4.52", "pandas>=2.0.0"],
    python_requires=">=3.10",
)
