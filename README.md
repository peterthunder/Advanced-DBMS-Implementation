# Advanced-DBMS-Implementation
Project for course Κ23a "Software Development for Information Systems".

This project uses the [RadixHashJoin methodology](https://ieeexplore.ieee.org/document/6544839) to Join two Tables.

# How to run
Navigate to the root-directory of this project and run the commands of your preferred choice:
1) For **BASIC** (fast) execution run:
    1) `make BASIC`
    2) `./radixHashJoin_basic`
2) For **DEBUG** (with prints) execution run:
    1) `make DEBUG`
    2) `./radixHashJoin_debug`

# Unit-testing
We apply unit-testing in our code by using the ["Unity" Framework](https://github.com/ThrowTheSwitch/Unity), which we include directly in "***UnitTesting/Unity-master***".
For **"Unity"** to work, we first need to install "**ruby**", "**rake**", "**libc6-dev-i386**" and "**g++-multilib**" by running the following commands in the terminal:<br/>
1) `sudo apt-get install ruby`
2) `sudo apt-get install rake`
3) `sudo apt-get install libc6-dev-i386`
4) `sudo apt-get install g++-multilib`

Then, in order to run the tests, you have to run the following command:<br/>
`make UNIT_TESTING`
