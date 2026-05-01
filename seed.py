"""Seed initial anime data so the platform is usable on first run. Python 3.8 compatible."""
from typing import Any, Dict, List

from sqlalchemy import select

from app.models.anime import Anime, Episode
from database import SessionLocal

# A public Big Buck Bunny stream we can reuse as a placeholder for episode video.
SAMPLE_VIDEO = (
    "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4"
)

SEED_ANIME: List[Dict[str, Any]] = [
    {
        "title": "Jujutsu Kaisen",
        "alternative_titles": "Магическая битва, JJK",
        "description": (
            "Юдзи Итадори — обычный школьник, который оказывается втянут в мир проклятий "
            "после того, как съедает палец легендарного демона Сукуны."
        ),
        "poster_url": "https://image.tmdb.org/t/p/w500/g1rK2SnMYWA6L5j5Dzr5g3T7zzv.jpg",
        "backdrop_url": "https://image.tmdb.org/t/p/original/oRRv3kZ4iaOX8jmPNtygmOHU24Z.jpg",
        "genres": "Action, Supernatural, Shounen",
        "year": 2020,
        "status": "ongoing",
        "rating": 8.7,
    },
    {
        "title": "One Piece",
        "alternative_titles": "Ван Пис",
        "description": (
            "Монки Д. Луффи отправляется в путешествие через Гранд Лайн, "
            "чтобы найти легендарное сокровище Ван Пис и стать королём пиратов."
        ),
        "poster_url": "https://image.tmdb.org/t/p/w500/cMD9Ygz11zjJzAovURpO75Qg7rT.jpg",
        "backdrop_url": "https://image.tmdb.org/t/p/original/jq7e9hTzMmmXhvxF8kFkR3IKv5T.jpg",
        "genres": "Action, Adventure, Fantasy",
        "year": 1999,
        "status": "ongoing",
        "rating": 9.0,
    },
    {
        "title": "Dandadan",
        "alternative_titles": "Дандадан",
        "description": (
            "Девочка, верящая в призраков, и парень, верящий в инопланетян, "
            "сталкиваются с обоими сразу — и теперь должны разобраться с этим вместе."
        ),
        "poster_url": "https://image.tmdb.org/t/p/w500/glyM8XO2DIPAZyJI0EMnbbANBig.jpg",
        "backdrop_url": "https://image.tmdb.org/t/p/original/aEM9BvgAYhvYSqQ5fqxwTbBKVPV.jpg",
        "genres": "Action, Comedy, Supernatural",
        "year": 2024,
        "status": "ongoing",
        "rating": 8.6,
    },
    {
        "title": "Chainsaw Man",
        "alternative_titles": "Человек-бензопила",
        "description": (
            "Дэндзи живёт в нищете, охотясь на демонов с маленьким бесом-бензопилой Почитой. "
            "После предательства он сливается с Почитой и становится Человеком-бензопилой."
        ),
        "poster_url": "https://image.tmdb.org/t/p/w500/npdB6eFzizki0WaZ1OvKcJrWe97.jpg",
        "backdrop_url": "https://image.tmdb.org/t/p/original/r5xVal1IPQPySSjLFzs9eP9pWl3.jpg",
        "genres": "Action, Horror, Supernatural",
        "year": 2022,
        "status": "completed",
        "rating": 8.5,
    },
    {
        "title": "Naruto",
        "alternative_titles": "Наруто",
        "description": (
            "Молодой ниндзя Наруто Узумаки мечтает стать Хокаге — самым сильным "
            "и уважаемым ниндзя в своей деревне Скрытого Листа."
        ),
        "poster_url": "https://image.tmdb.org/t/p/w500/xppeysfvDKVx775MFuH8Z9BlpMk.jpg",
        "backdrop_url": "https://image.tmdb.org/t/p/original/q41jKK0dKLEnNJZJ5G9JPsIkSc7.jpg",
        "genres": "Action, Adventure, Shounen",
        "year": 2002,
        "status": "completed",
        "rating": 8.4,
    },
]


async def seed_if_empty() -> None:
    async with SessionLocal() as session:
        existing = await session.execute(select(Anime))
        if existing.scalars().first() is not None:
            return

        for entry in SEED_ANIME:
            anime = Anime(**entry)
            session.add(anime)
            await session.flush()  # get anime.id

            for ep_num in range(1, 4):
                episode = Episode(
                    anime_id=anime.id,
                    episode_number=ep_num,
                    title="Эпизод {}".format(ep_num),
                    video_url=SAMPLE_VIDEO,
                )
                session.add(episode)

        await session.commit()
