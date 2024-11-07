#!/bin/sh

python -m scripts.download_models

# English language LR model
curl -SL https://tfhub.dev/google/universal-sentence-encoder/2?tf-hub-format=compressed -o use2.tar.gz
tar xfz use2.tar.gz -C files/models/embeddings-en

# Multilingual language LR model (for Korean)
curl -SL https://www.kaggle.com/api/v1/models/google/universal-sentence-encoder/tensorFlow2/multilingual/2/download -o usem2.tar.gz
tar xfz usem2.tar.gz -C files/models/embeddings-multi
