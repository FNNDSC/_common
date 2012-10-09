#
# ChRIS reloaded - ANSI Colors
#
# (c) 2012 FNNDSC, Boston Children's Hospital
#
import types

class Colors( object ):
  '''
  ANSI colors.
  '''
  YELLOW = '\033[33m'
  PURPLE = '\033[35m'
  RED = '\033[31m'
  ORANGE = '\033[93m'
  GREEN = '\033[32m'
  CYAN = '\033[36m'
  _CLEAR = '\033[0m'

  @staticmethod
  def strip( text ):
    '''
    Strips all color codes from a text.
    '''
    members = [attr for attr in Colors.__dict__.keys() if not attr.startswith( "__" ) and not attr == 'strip']

    for c in members:

      text = text.replace( vars( Colors )[c], '' )

    return text
