import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kiltsreader",
    version="0.0.1",
    author="Christopher Conlon and Chitra Marti",
    author_email="cconlon@stern.nyu.edu",
    description="A package for reading Kilts NielsenIQ data files and directories",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chrisconlon/kiltsnielsen",
    project_urls={
        "Bug Tracker": "https://github.com/chrisconlon/kiltsnielsen/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "kiltsreader"},
    packages=setuptools.find_packages(where=""),
    install_requires=['pyarrow >= 5.0.0','pandas >= 1.2','numpy>= 1.2'],
    python_requires='>=3.6',
)
