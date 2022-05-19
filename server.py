from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route


async def homepage(request):
    return JSONResponse({'message': 'root page for sms-parser'})

async def parse(request):
    if request.method == 'GET':
        return JSONResponse({'message': 'use POST'})
    
    elif request.method == 'POST':
        return JSONResponse({'message': 'where\'s your payload'})
    
    else:
        return JSONResponse({
            'success': False,
            'message': 'METHOD NOT SUPPORTED'
        })

app = Starlette(debug=True, routes=[
    Route('/', homepage),
    Route('/parse', parse)
])