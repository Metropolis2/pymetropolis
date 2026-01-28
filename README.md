<a id="readme-top"></a>
<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![GPL v3][license-shield]][license-url]
<!-- [![LinkedIn][linkedin-shield]][linkedin-url] -->



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/Metropolis2/pymetropolis">
    <img src="icons/80x80.png" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">Pymetropolis</h3>

  <p align="center">
      Pymetropolis is a Python pipeline to generate, calibrate, run and analyze METROPOLIS2 simulation instances.
    <br />
    <a href="https://docs.metropolis2.org"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://metropolis2.org">Website</a>
    &middot;
    <a href="https://github.com/Metropolis2/pymetropolis/issues/new?labels=bug&template=bug_report.yml">Report Bug</a>
    &middot;
    <a href="https://github.com/Metropolis2/pymetropolis/issues/new?labels=enhancement&template=feature_request.yml">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <!--<li><a href="#citation">Citation</a></li>-->
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#semver">Semver</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

[![METROPOLIS2 example output][product-screenshot]](https://metropolis2.org)

METROPOLIS2 is a dynamic multi-modal agent-based transport simulator.

<!-- TODO add graph of project structure -->

<!--### Citation

If you use this project in your research, please cite it as follows:

de Palma, A. & Javaudin, L. (2025). _METROPOLIS2_. [https://metropolis2.org](https://metropolis2.org)

Javaudin, L., & de Palma, A. (2024). _METROPOLIS2: Bridging theory and simulation in agent-based transport modeling._ Technical report, THEMA (THéorie Economique, Modélisation et Applications).

_Refer to [CITATION.cff](CITATION.cff) and [CITATION.bib](CITATION.bib) for details._
-->

### Built With

[![Python][Python]][Python-url]

Pymetropolis make use of some great Python libraries, including:

- [geopandas](https://geopandas.org/) for geospatial data manipulation
- [loguru](https://loguru.readthedocs.io/) for logging
- [matplotlib](https://matplotlib.org/) for data visualization
- [networkx](https://networkx.org/) for graph manipulation
- [numpy](https://numpy.org/) for arrays and random number generators
- [pyosmium](https://osmcode.org/pyosmium/) for OpenStreetMap data manipulation
- [polars](https://pola.rs/) for extremely fast dataframes
- [shapely](https://shapely.readthedocs.io/) for geometric objects
- [typer](https://typer.tiangolo.com/) for easy CLI

### Semver

Pymetropolis is following [Semantic Versioning 2.0](https://semver.org/).

Each new version is given a number MAJOR.MINOR.PATCH.
An increase of the MAJOR number indicates backward incompatibilities with previous versions.
An increase of the MINOR number indicates new features, that are backward-compatible.
An increase of the PATCH number indicates bug fixes.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

1. Install the Python package with `pip install pymetropolis`.
2. Download the [Metropolis-Core simulator](https://github.com/Metropolis2/Metropolis-Core/releases).
3. Create a TOML configuration file describing the simulation instance.
4. Run the pipeline with `pymetropolis my-config.toml`.

For more details, please refer to the
[documentation](https://docs.metropolis2.org/pymetropolis/getting_started/index.html).
You can find complete examples of simulation instances in the
[official case studies](https://docs.metropolis2.org/pymetropolis/case_study/index.html).

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTRIBUTING -->
## Contributing

If you would like to add a feature to Pymetropolis, start by opening an issue with the tag
"enhancement" so that we can discuss its feasibility.

If your suggestion is accepted, you can then create a Pull Request:

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

_For more details, please read [CONTRIBUTING.md](CONTRIBUTING.md)
and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)._

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ### Top contributors:

<a href="https://github.com/Metropolis2/pymetropolis/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Metropolis2/pymetropolis" alt="contrib.rocks image" />
</a>
-->



<!-- LICENSE -->
## License

Pymetropolis is free and open-source software licensed under the
[GNU General Public License v3.0](https://www.gnu.org/licenses/).

You are free to:

- Modify and redistribute this software
- Use it for any purpose, personal or commercial

Under the following conditions:

- You retain the original copyright notice
- You distribute you modifications under the same license (GPL-3.0 or later)
- You document any significant changes you make

For the full license text and legal details, see the `LICENSE.txt` file.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

If you have any questions, either post an
[issue](https://github.com/Metropolis2/pymetropolis/issues)
or send an e-mail to any of these addresses:

- METROPOLIS2 team - contact@metropolis2.org
- Lucas Javaudin - metropolis@lucasjavaudin.com

Project Link: [https://github.com/Metropolis2/pymetropolis](https://github.com/Metropolis2/pymetropolis)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Pymetropolis benefited from the work of Kokouvi Joseph Djafon on the calibration tools.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/Metropolis2/pymetropolis.svg?style=for-the-badge
[contributors-url]: https://github.com/Metropolis2/pymetropolis/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Metropolis2/pymetropolis.svg?style=for-the-badge
[forks-url]: https://github.com/Metropolis2/pymetropolis/network/members
[stars-shield]: https://img.shields.io/github/stars/Metropolis2/pymetropolis.svg?style=for-the-badge
[stars-url]: https://github.com/Metropolis2/pymetropolis/stargazers
[issues-shield]: https://img.shields.io/github/issues/Metropolis2/pymetropolis.svg?style=for-the-badge
[issues-url]: https://github.com/Metropolis2/pymetropolis/issues
[license-shield]: https://img.shields.io/github/license/Metropolis2/pymetropolis.svg?style=for-the-badge
[license-url]: https://github.com/Metropolis2/pymetropolis/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/lucas-javaudin
[product-screenshot]: images/traffic_flows.jpg
<!-- Shields.io badges. You can a comprehensive list with many more badges at: https://github.com/inttter/md-badges -->
[Python]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://www.python.org/
