import json

from aiohttp import web
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import Column, Integer, String, DateTime, func, select
from sqlalchemy.ext.declarative import declarative_base


engine = create_async_engine('postgresql+asyncpg://app:1234@127.0.0.1:5431/data')
Session = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()


class Advertisement(Base):
    __tablename__ = "advertisement"

    id = Column(Integer, primary_key=True)
    header = Column(String(32), nullable=False)
    description = Column(String(32), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    owner = Column(Integer, nullable=False)


def raise_http_error(error_class, message):
    raise error_class(
        text=json.dumps({"status": "error", "description": message}),
        content_type="application/json",
    )


@web.middleware
async def session_middleware(request, handler):
    async with Session() as session:
        request["session"] = session
        return await handler(request)


class AdvertisementView(web.View):
    async def get(self):
        adv_id = int(self.request.match_info.get("adv_id", 0))
        if adv_id:
            advertisement = await self.request["session"].get(Advertisement, adv_id)
            if advertisement is None:
                raise raise_http_error(web.HTTPNotFound, f"{Advertisement.__name__} not found")
            return web.json_response({
                'id': advertisement.id,
                'header': advertisement.header,
                'description': advertisement.description,
                'created_at': advertisement.created_at.isoformat(),
                'owner': advertisement.owner
            })
        else:
            advertisements = await self.request["session"].execute(select(Advertisement))  # не нравится
            advertisements = advertisements.scalars().all()
            answer = {}
            if advertisements:
                for num, adv in enumerate(advertisements):
                    answer[f'{num}'] = {
                        'id': adv.id,
                        'header': adv.header,
                        'description': adv.description,
                        'created_at': adv.created_at.isoformat(),
                        'owner': adv.owner
                    }
        return web.json_response({
            'response': {'count': len(advertisements), 'items': answer}
        })

    async def post(self):
        load_data = await self.request.json()
        new_adv = Advertisement(**load_data)
        self.request["session"].add(new_adv)
        await self.request["session"].commit()
        response = web.json_response({"message": "add new advertisement", "id": new_adv.id})
        response.set_status(status=201)
        return response

    async def patch(self):
        adv_id = int(self.request.match_info.get("adv_id"))
        updated_data = await self.request.json()
        advertisement = await self.request["session"].get(Advertisement, adv_id)
        if advertisement is None:
            raise raise_http_error(web.HTTPNotFound, f"{Advertisement.__name__} not found")
        for field, value in updated_data.items():
            setattr(advertisement, field, value)
        self.request["session"].add(advertisement)
        await self.request["session"].commit()
        return web.json_response({"message": f"advertisement №{adv_id} updated"})

    async def delete(self):
        adv_id = int(self.request.match_info.get("adv_id"))
        adv = await self.request["session"].get(Advertisement, adv_id)
        if adv is None:
            raise raise_http_error(web.HTTPNotFound, f"{Advertisement.__name__} not found")
        await self.request["session"].delete(adv)
        await self.request["session"].commit()
        response = web.Response()
        response.set_status(status=204)
        return response


async def app_context(app: web.Application):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


app = web.Application(middlewares=[session_middleware])
app.cleanup_ctx.append(app_context)
app.add_routes(
    [
        web.post("/api/v1/advertisement/", AdvertisementView),
        web.get(r"/api/v1/advertisement/{adv_id:\d+}", AdvertisementView),
        web.get(r"/api/v1/advertisement/", AdvertisementView),
        web.patch(r"/api/v1/advertisement/{adv_id:\d+}", AdvertisementView),
        web.delete(r"/api/v1/advertisement/{adv_id:\d+}", AdvertisementView),
    ]
)

if __name__ == '__main__':
    web.run_app(app)
