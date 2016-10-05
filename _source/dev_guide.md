---
layout: site
title: DevGuide
---
# Reprowd DevGuide

## Welcome

Reprowd is a python package which aims to make it easy to reproduce crowdsourced data processing research.

## Bug Reports

If you encounter a problem with Reprowd or have an idea for new features, please submit it to the [issue tracker](https://github.com/sfu-db/reprowd/issues) on Github,

## Contributing

The way for new contributors to submit code to Reprowd is to fork the repository on Github and then submit a pull request after committing the changes.

The pull request will then need to be approved by one of the core developers before it is merged into the main repository.

## Getting Started

These are the basic steps needed to start developing on Reprowd.

1. [Downloading and Installation](install.html)
2. Create an account on Github.
3. Fork the Reprowd repository
4. Clone the forked repository to your machine.

		$git clone https://github.com/[your_github_user_id]/reprowd.git
		$cd reprowd
5. Check out the appropriate branch.
6. Creating a new working branch.

		$git checkout -b test
6. <b>Optional:</b> setup a virtual enviroment.

		$virtualenv ~/reprowd
		$. ~/reprowd/bin/active
		$pip install pybossa-client
7. Develop.
8. Test.
	See [nosetests](http://nose.readthedocs.io/en/latest/)

		$cd tests
		$nosetests
9. Push changes in the branch to your forked repository.

		$git push origin test
10. Submit a pull request.
11. Wait for a core developer to review your changes

## Contact

[Jiannan Wang](jnwang@sfu.ca)

[Ruochen Jiang](ruochenj@sfu.ca)
