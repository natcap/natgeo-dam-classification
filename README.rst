Validate Dam Locations
======================

This project contains two Python Flask apps:

* `data_validator.py` presents a web interface to allow users
  to manually construct bounding boxes around possible dam locations known from
  a variety of public data sources.

  The entry point is `data_validator.py` and can be launched with:

  `python data_validator.py`

* `not_a_dam_fetcher.py` presents a web interface that allows users to click
  "dam/no dam" to build up a set of imagery that does not contain images of
  dams.
