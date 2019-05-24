from setuptools import find_packages, setup


install_requires = (
    "aiohttp>=3.5",
    "async-generator==1.9",
    'dataclasses==0.6; python_version<"3.7"',
    "yarl>=1.3.0",
)

setup(
    name="platform-monitoring",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-monitoring",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": ["platform-monitoring=platform_monitoring.api:main"]
    },
    zip_safe=False,
)
