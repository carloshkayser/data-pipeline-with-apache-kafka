---
marp: true
theme: gaia
_class: lead
title: Data Stream Processing with Apache Kafka and Spark Structured Streaming
description: An example slide deck created using Marpit

author: "Carlos Henrique Kayser"
date: "April 2022"

paginate: true
backgroundColor: #fff
# backgroundImage: url('https://marp.app/assets/hero-background.svg')

---

<style>
h1 { font-size: 1.5rem }

p { font-size: 80% }

img[alt~="center"] {
  display: block;
  margin: 0 auto;
}
figcaption {
  display: block;
  margin: 0 auto;
}
</style>

Pontifical Catholic University of Rio Grande do Sul<br>Graduate Program in Computer Science

# Data Stream Processing with Apache Kafka<br>and Spark Structured Streaming

Carlos Henrique Kayser<br>Email: carlos.kayser@edu.pucrs.br

Scalable Data Stream Processing<br>Prof. Dr. Dalvan Jair Griebler</p>

May 14th, 2022

---

# Apache Kafka

---

# Apache Kafka Topic

![w:600 center](figures/kafka-topic.png)
*Fig.1 - 4K Mountains Wallpaper*


---

<!-- <p>
    <img src="figures/kafka-topic.png" alt style="width:50%">
</p>
<p>
    <em>image_caption</em>
</p> -->


<figure class="image">
  <img src="figures/kafka-topic.png" alt="Descirp" style="width:50%">
  <figcaption>Kafka Topic</figcaption>
</figure>


---

<style>
.image-caption {
  text-align: center;
  font-size: .8rem;
  color: light-grey;
}
</style>

![w:600 center](figures/kafka-topic.png)

<figcaption>Kafka Topic</figcaption>



















---

# Apache Spark Structured Streaming

---

<figure>

<img src="figures/kafka-topic.png" alt="Trulli" style="width:50%">
<figcaption style="align=center"><b>Fig.1 - 4K Mountains Wallpaper</b></figcaption>

</figure>

---
Render inline math such as $ax^2+bc+c$.

$$ I_{xx}=\int\int_Ry^2f(x,y)\cdot{}dydx $$

$$
f(x) = \int_{-\infty}^\infty
    \hat f(\xi)\,e^{2 \pi i \xi x}
    \,d\xi
$$

---

# I am a slide

```python
query = results_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "predictions") \
    .option("checkpointLocation", "/home/kayser/temp") \
    .outputMode("Append") \
    .start()

time.sleep(30)

query.stop()
```

---

# Document Title

<style scoped>
{
  font-size: 13px
}
</style>

The usual [Markdown Cheatsheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
does not cover some of the more advanced Markdown tricks, but here
is one. You can combine verbatim HTML with your Markdown. 
This is particularly useful for tables.
Notice that with **empty separating lines** we can use Markdown inside HTML:

<table>
<tr>
<th>Json 1</th>
<th>Markdown</th>
</tr>
<tr>
<td>
  
```json
{
  "id": 1,
  "username": "joe",
  "email": "joe@example.com",
  "order_id": "3544fc0"
}
```
  
</td>
<td>

```json
{
  "id": 5,
  "username": "mary",
  "email": "mary@example.com",
  "order_id": "f7177da"
}
```

</td>
</tr>
</table>

---
# I am slide

<style scoped>
pre {
   font-size: 2rem;
}
</style>

```cs
// I am code block
```

--- 

---
# I am slide

<style scoped>
pre {
   font-size: 2rem;
}
</style>s

```cs
// I am code block
```

--- 
# Slide 1 title

Some super quickly created demo slides

* Do not need anything else than markdown
    * Slides title starts with # (also starts a new slide)
    * Bullet points, newlines, empty lines: all standard markdown
* However, can also use other stuff, e.g.:
    * Some HTML (e.g. \<center\>)
    * When using pandoc beamer, can use latex commands (e.g. \\center, \\large, etc)\dots

---
# Slide 2 title

\center The slide syntax is so simple that you can quickly create a handful of slides on basically any device in any editor. E.g. on your mobile on the way to the meeting where you need the slides. Right before the meeting starts you use pandoc to create the actual slides from your source.
