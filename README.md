![Banner](banner.svg)
<!-- See https://github.com/rmariuzzo/github-banner -->

# High streets

---

**_NOTE this is just a template for writing your project readme, you can find plenty more of other options to choose
from [here](https://www.readme-templates.com/)_**

Here is an [Awesome-Readme git hub repoisory](https://github.com/matiassingers/awesome-readme)

Author: Conor Dempsey

<!-- Describe your project in brief -->

Analysis and modeling of high street profiles. 

The project title should be self-explanatory and try not to make it a mouthful. (Although exceptions exist)

I use [**Shields IO**](https://shields.io/) for making badges.

<!-- Add badges with link to Shields IO -->
![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/Greater-London-Authority/highstreets?include_prereleases)
![GitHub last commit](https://img.shields.io/github/last-commit/Greater-London-Authority/highstreets)
![GitHub issues](https://img.shields.io/github/issues-raw/Greater-London-Authority/highstreets)
![GitHub pull requests](https://img.shields.io/github/issues-pr/Greater-London-Authority/highstreets)
![GitHub](https://img.shields.io/github/license/Greater-London-Authority/highstreets)

# Demo-Preview

---

<!-- Add a demo for your project -->

After you have written about your project, it is a good idea to have a demo/preview(**video/gif/screenshots** are good
options) of your project so that people can know what to expect in your project. You could also add the demo in the
previous section with the product description.

# Table of contents

---

After you have introduced your project, it is a good idea to add a **Table of contents** or **TOC** as **cool** people
say it. This would make it easier for people to navigate through your README and find exactly what they are looking for.


- [Project Title](#project-title)
- [Demo-Preview](#demo-preview)
- [Table of contents](#table-of-contents)
- [Installation](#installation)
- [Usage](#usage)
- [Development](#development)
- [Contribute](#contribute)
    - [Adding new features or fixing bugs](#adding-new-features-or-fixing-bugs)
- [License](#license)
- [Contact](#contact)
- [Acknowledgements](#acknowledgements)
- [Footer](#footer)

# Installation

[(Back to top)](#table-of-contents)

*You might have noticed the **Back to top** button(if not, please notice, it's right there!). This is a good idea
because it makes your README **easy to navigate.***

The first section should be how to install(how to generally use your project or set-up for editing in their machine).

This should give the users a concrete idea with instructions on how they can use your project repo with all the steps.

Following this steps, **they should be able to run this in their device.**

A method I use is after completing the README, I go through the instructions from scratch and check if it is working.

Here is a sample instruction:

To use this project, first clone the repo on your device using the command below:

```git init```

```
git clone https://github.com/Greater-London-Authority/highstreets

# Install package requirements
conda env create -f environment.yml
```


# Usage

[(Back to top)](#table-of-contents)

This is optional and it is used to give the user info on how to use the project after installation. This could be added
in the Installation section also.

# Development

[(Back to top)](#table-of-contents)

This is the place where you give instructions to developers on how to modify the code.

You could give **instructions in depth** of **how the code works** and how everything is put together.

You could also give specific instructions to how they can setup their development environment.

Ideally, you should keep the README simple. If you need to add more complex explanations, use a wiki. Check
out [this wiki](https://github.com/navendu-pottekkat/nsfw-filter/wiki) for inspiration.

Useful advice and a demonstration below.

## Extra packages to install

The package requires geopandas and, which cannot be installed directly with pip on 
windows. Installing with conda resolves these issues, so if your current 
environment does not contain geopandas you will need to install it first with the following command:

```
conda install -c conda-forge geopandas gdal
```

glapy itself can then be installed with (git needs to be installed):

```
pip install git+https://github.com/Greater-London-Authority/glapy
```

As this is a private repo you will be prompted to enter your git username and password.

If you are using pip or poetry if you want pre-commit or gdal
```shell
sudo apt-get install gdal-bin
sudo apt-get install libgdal-dev
pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`

pip install --user --upgrade pre-commit
```

# Contribute

[(Back to top)](#table-of-contents)

This is where you can let people know how they can **contribute** to your project. Some of the ways are given below.

Also this shows how you can add subsections within a section.

### Adding new features or fixing bugs

[(Back to top)](#table-of-contents)

This is to give people an idea how they can raise issues or feature requests in your projects.

You could also give guidelines for submitting and issue or a pull request to your project.

You could also add contact details for people to get in touch with you regarding your project.

# License

[(Back to top)](#table-of-contents)

Adding the license to README is a good practice so that people can easily refer to it.

Make sure you have added a LICENSE file in your project folder. **Shortcut:** Click add new file in your root of your
repo in GitHub --> Set file name to LICENSE --> GitHub shows LICENSE templates ---> Choose the one that best suits your
project!

# Contact

Conor Dempsey - conor.dempsey@london.gov.uk

# Acknowledgements

Any other general acknowledgements or packages used.

# Footer

[(Back to top)](#table-of-contents)

Footer's aren't necessary but you **can** use this to convey important info.


<!-- Add the footer here ![Footer](https://github.com/Greater-London-Authority/highstreets/blob/master/fooooooter.png) -->

