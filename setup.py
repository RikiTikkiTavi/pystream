from distutils.core import setup

setup(
    name='pystream',  # How you named your package folder (MyLib)
    packages=['pystream'],  # Chose the same as "name"
    version='0.2',  # Start with a small number and increase it with every change you make
    license='MIT',  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description='Java Stream Api analogue.',  # Give a short description about your library
    author='Yahor Paromau',  # Type in your name
    author_email='yahor.paromau@gmail.com',  # Type in your E-Mail
    url='https://github.com/RikiTikkiTavi/PyStream-API',  # Provide either the link to your github or to your website
    download_url='https://github.com/RikiTikkiTavi/PyStream-API/archive/v_01.tar.gz',  # I explain this later on
    keywords=['stream-api', 'stream', 'java stream-api'],  # Keywords that define your package best
    classifiers=[
        'Development Status :: 4 - Beta',
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',  # Define that your audience are developers
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',  # Again, pick a license
        'Programming Language :: Python :: 3.8',
    ],
)
