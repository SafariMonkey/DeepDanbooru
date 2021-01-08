# Notes

## Ideas

* Captcha-like tag confidence adjustment by asking about images in a wide range of confidence scores.
    * This allows the network to compensate for tags that have a low recall in the original dataset

* Provide the network with a random sample of the real tags
    * This could be only some of the time (50%?)
    * They could be weighted by frequency, as infrequent tags are more likely to have a low recall in the original dataset.

* Alternatively, combine a covariance matrix with the network by combining the probabilities given by the network with probabilities output by the covariance matrix given the known tags

