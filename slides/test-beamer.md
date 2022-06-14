% Title
% Author
% Date

# Slide 1 title

Some super quickly created demo slides

* Do not need anything else than markdown
    * Slides title starts with # (also starts a new slide)
    * Bullet points, newlines, empty lines: all standard markdown
* However, can also use other stuff, e.g.:
    * Some HTML (e.g. \<center\>)
    * When using pandoc beamer, can use latex commands (e.g. \\center, \\large, etc)\dots

# Slide 2 title

\center The slide syntax is so simple that you can quickly create a handful of slides on basically any device in any editor. E.g. on your mobile on the way to the meeting where you need the slides. Right before the meeting starts you use pandoc to create the actual slides from your source.


<!-- pandoc -t beamer sides.md -o slides.pdf -->