from setuptools import find_packages, setup

setup(
    name="Store_Sales_Forecasting_Model_Decay_Simulation",
    packages=find_packages(exclude=["Store_Sales_Forecasting_Model_Decay_Simulation_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
