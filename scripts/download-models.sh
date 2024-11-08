#!/bin/sh

#python -m scripts.download_models

# English language LR model
# originally: curl -SL https://storage.googleapis.com/tfhub-modules/google/universal-sentence-encoder/4.tar.gz -o 4.tar.gz
curl -SL "https://www.dropbox.com/s/6vfe4ttvoypisp5/embeddings-en.tar.gz?dl=1" -o embeddings-en.tar.gz
tar xfz embeddings-en.tar.gz -C files/models/

# Multilingual language LR model (for Korean)
# originally: curl -SL https://storage.googleapis.com/tfhub-modules/google/universal-sentence-encoder-multilingual/3.tar.gz -o 3.tar.gz
curl -SL "https://www.dropbox.com/scl/fi/wrpg8tqgqb9boqgwf07ec/embeddings-multi.tar.gz?rlkey=rxtqq8ggfls4jh00rrdurs5wx&dl=1" -o embeddings-multi.tar.gz
tar xfz embeddings-multi.tar.gz -C files/models/
