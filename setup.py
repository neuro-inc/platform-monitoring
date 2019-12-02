from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.6.2",
    "yarl==1.4.1",
    "neuro_auth_client==19.10.5",
    "neuromation==19.7.4",
    "aioelasticsearch==0.5.2",
    "aiodocker==0.17.0",
    "docker-image-py==0.1.10",
    "trafaret==2.0.1",
    "platform-logging==0.3",
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
