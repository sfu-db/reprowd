# CrowdBase DevGuide

##Welcome

CrowdBase is a python package which offers a fault-tolerant and reproducible environment for doing crowdsourced data processing tasks based on [Pybossa Server](http://pybossa.com/)

##Bug Reports

If you have encounter a problem with CrowdBase or have an idea for new features, please submit it to the [issue tracker](https://github.com/sfu-db/crowdbase/issues) on Github,

##Contributing

The way for new contributors to submit code to CrowdBase is to fork the repository on Github and then submit a pull request after committing the changes.

The pull request will then need to be approved by one of the core developers before it is merged into the main repository.

##Getting Started

There are the basic steps needed to start developing on CrowdBase.

1. Install your own [Pybossa Server](http://docs.pybossa.com/en/latest/juju_pybossa.html)
2. Create an account on Github.
3. Fork the CrowdBase repository
4. Clone the forked repository to your machine.

		git clone https://github.com/sfu-db/crowdbase.git
		cd crowbase
5. Check out the appropriate branch.
6. Creating a new working branch.

		git checkout -b test
6. <b>Optional:</b> setup a virtual enviroment.

		virtualenv ~/crowdbase
		. ~/crowdbase/bin/active
		pip install pybossa-client
7. Develop.
	See [Programming Doc]() for tips.
8. Test.
9. Push changes in the branch to your forked repository.

		git push origin test
10. Submit a pull request.
11. Wait for a core developer to review your changes

##Contact

[Jiannan Wang](jnwang@sfu.ca)

[Ruochen Jiang](ruochenj@sfu.ca)
