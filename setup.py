from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.6.2",
    "yarl==1.5.1",
    "neuro_auth_client==19.10.5",
    "neuromation==20.9.3",
    "aioelasticsearch==0.7.0",
    "aiodocker==0.19.1",
    "docker-image-py==0.1.10",
    "trafaret==2.0.2",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "aiobotocore==1.1.1",
)

setup(
    name="platform-monitoring",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-monitoring",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": ["platform-monitoring=platform_monitoring.api:main"]
    },
    zip_safe=False,
)
