# IcePack/__init__.py
import os

if not globals().get('_banner_printed'):
    if os.getenv("ICEPACK_BANNER", "1") == "1":
      print(r"""
IIIIIIIIII      CCCCCCCCCCCCCEEEEEEEEEEEEEEEEEEEEEEPPPPPPPPPPPPPPPPP        AAA                  CCCCCCCCCCCCCKKKKKKKKK    KKKKKKK
I::::::::I   CCC::::::::::::CE::::::::::::::::::::EP::::::::::::::::P      A:::A              CCC::::::::::::CK:::::::K    K:::::K
I::::::::I CC:::::::::::::::CE::::::::::::::::::::EP::::::PPPPPP:::::P    A:::::A           CC:::::::::::::::CK:::::::K    K:::::K
II::::::IIC:::::CCCCCCCC::::CEE::::::EEEEEEEEE::::EPP:::::P     P:::::P  A:::::::A         C:::::CCCCCCCC::::CK:::::::K   K::::::K
  I::::I C:::::C       CCCCCC  E:::::E       EEEEEE  P::::P     P:::::P A:::::::::A       C:::::C       CCCCCCKK::::::K  K:::::KKK
  I::::IC:::::C                E:::::E               P::::P     P:::::PA:::::A:::::A     C:::::C                K:::::K K:::::K   
  I::::IC:::::C                E::::::EEEEEEEEEE     P::::PPPPPP:::::PA:::::A A:::::A    C:::::C                K::::::K:::::K    
  I::::IC:::::C                E:::::::::::::::E     P:::::::::::::PPA:::::A   A:::::A   C:::::C                K:::::::::::K     
  I::::IC:::::C                E:::::::::::::::E     P::::PPPPPPPPP A:::::A     A:::::A  C:::::C                K:::::::::::K     
  I::::IC:::::C                E::::::EEEEEEEEEE     P::::P        A:::::AAAAAAAAA:::::A C:::::C                K::::::K:::::K    
  I::::IC:::::C                E:::::E               P::::P       A:::::::::::::::::::::AC:::::C                K:::::K K:::::K   
  I::::I C:::::C       CCCCCC  E:::::E       EEEEEE  P::::P      A:::::AAAAAAAAAAAAA:::::AC:::::C       CCCCCCKK::::::K  K:::::KKK
II::::::IIC:::::CCCCCCCC::::CEE::::::EEEEEEEE:::::EPP::::::PP   A:::::A             A:::::AC:::::CCCCCCCC::::CK:::::::K   K::::::K
I::::::::I CC:::::::::::::::CE::::::::::::::::::::EP::::::::P  A:::::A               A:::::ACC:::::::::::::::CK:::::::K    K:::::K
I::::::::I   CCC::::::::::::CE::::::::::::::::::::EP::::::::P A:::::A                 A:::::A CCC::::::::::::CK:::::::K    K:::::K
IIIIIIIIII      CCCCCCCCCCCCCEEEEEEEEEEEEEEEEEEEEEEPPPPPPPPPPAAAAAAA                   AAAAAAA   CCCCCCCCCCCCCKKKKKKKKK    KKKKKKK                                                                                                                                                                                         
      """)
    globals()['_banner_printed'] = True



