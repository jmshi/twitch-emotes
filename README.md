<p align="center">
<img style="float" src="img/banner.png">
</p>

[link_to_slides](https://www.slideshare.net/slideshow/embed_code/key/gGbeGLC0j0CvZO)

[link_to_demo_video](https://youtu.be/wzxTnE7EMcE)


# Code Description
A real-time data pipeline for analyzing ztwitch chat data. Specially designed to track and analyze the global (free) and subscribed 
(paid) emotes embedded in chat messages from up to 1000 active channels. Written mostly in python.

# Motivation
More than 300 general emotes are free to use for any streaming channels in Twitch.tv, however, there are around 1 Million of unique paid emotes users have to pay monthly in order to use in twitch. The subscription fee of the latter actually contribute a lot to the income of not only Twitch but also the gamers. Tracking and analyzing the paid/free meotes usage in real-time is therefore help twitch/gamers to form better marketing plans and business strategies. 

# Goals
For demonstrations, this pipeline is built to fullfill the following three purposes:
1) Find out the relative popularity of paid vs free emotes for various channel;
2) Find out the most popular paid and free emotes for a given channel;
3) Find the trending emotes of a specific topic, say WorldCup 2018

down vote

This will display the three images side by side if the images are not to wide.

<p float="left">
  <img src="img/channel.png" width="100" />
  <img src="img/emotes.png" width="100" /> 
  <img src="img/live.png" width="100" />
   <img src="img/worldcup.png" width="100" />
</p>


# Pipeline Architecture
<p align="center">
<img style="float" src="img/pipeline.png">
</p>


