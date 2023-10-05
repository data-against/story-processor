# Additional Set Up Instructions

## Apple Silicon

If you ran into the error: `zsh: illegal hardware instruction ...` after following the set up instructions on the `README.md` file, there might be an issue related to Tensorflow. These instructions might resolve the problem:

1. Uninstall current installed Tensorflow: `pip uninstall tensorflow`
2. Make sure that Xcode command-line tools is installed: `xcode-select --install`
3. Install Tensorflow for MacOS: `pip install tensorflow-macos`
4. Install tensorflow-metal plug-in for MacOS: `pip install tensorflow-metal`
5. Run `pip list | grep tensorflow` and verify that the desired version of Tensorflow is installed, refer back to instructions on `README.md`if tensorflow-text is not installed
6. Run `pytest` to run all the automated tests and verify setup is working

## Windows

The process for setup described in the readme file is what needs to be followed.
1. Setup environment and install requirements. If you run into encodings error when pytest is run, add encoding='utf-8' where the file is being accessed. This will solve that error.
2. RabbitMQ and PostgreSQL needs to be installed seperately and the database must be created and the rest of the procedure is same.
3. After this run `pytest` to run all the automated tests and verify setup is working 
