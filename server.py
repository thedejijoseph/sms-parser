from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.endpoints import HTTPEndpoint
from starlette.routing import Route
from starlette.exceptions import HTTPException
from starlette.requests import Request

import sms_parser


async def not_found(request: Request, exc: HTTPException):
    return JSONResponse({
        'success': False,
        'error': 'Not found'
    },
    status_code=404)

async def server_error(request: Request, exc: HTTPException):
    return JSONResponse({
        'success': False,
        'error': 'Internal server error'
    },
    status_code=500)


exception_handlers = {
    404: not_found,
    500: server_error
}

async def homepage(request):
    return JSONResponse({'message': 'root page for sms-parser'})

class Parse(HTTPEndpoint):
    async def post(self, request):
        payload = await request.json()
        result = sms_parser.parse(payload)
        if result.get('error'):
            return JSONResponse({
                'success': False,
                'error': result['error'],
                'payload': payload
            })
        
        else:
            return JSONResponse({
                'success': True,
                'message': 'successfully parsed',
                'payload': payload,
                'data': result
            })

app = Starlette(debug=False, routes=[
    Route('/', homepage),
    Route('/parse', Parse)
],
exception_handlers=exception_handlers)
