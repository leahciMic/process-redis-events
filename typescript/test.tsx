import React, { useState } from 'react'
import './styles.css'

interface Card {
  id: number
  title: string
  description: string
  tags: string[]
  color: string
}

// Generate story cards
const generateCards = (count: number): Card[] => {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    title: `Story Card ${i + 1}`,
    description: `This is a description for story card ${
      i + 1
    }. It contains some text that describes the story element.`,
    tags: ['character', 'plot', 'scene'].slice(
      0,
      Math.floor(Math.random() * 3) + 1
    ),
    color: ['blue', 'green', 'purple', 'yellow'][i % 4]
  }))
}

interface CardProps {
  card: Card
  allCards: Card[]
  onUpdate: (id: number, newData: Card) => void
  searchTerm: string
}

function CardComponent({ card, allCards, onUpdate, searchTerm }: CardProps) {
  console.log('Rendering card', card.id)

  const relatedCount = allCards.filter(
    (c) => c.tags.some((tag) => card.tags.includes(tag)) && c.id !== card.id
  ).length

  return (
    <div key={card.id} className={`card card-${card.color}`} draggable="true">
      <div className="card-title">{card.title}</div>
      <div className="card-description">{card.description}</div>
      <div className="card-tags">
        {card.tags.map((tag) => (
          <span key={tag} className="tag">
            {tag}
          </span>
        ))}
      </div>
      <div className="card-related">{relatedCount} related cards</div>
      <button
        onClick={() =>
          onUpdate(card.id, { ...card, title: card.title + ' (updated)' })
        }
        className="card-button"
      >
        Update Title
      </button>
    </div>
  )
}

interface BoardProps {
  cards: Card[]
  onUpdate: (id: number, newData: Card) => void
  searchTerm: string
}

function Board({ cards, onUpdate, searchTerm }: BoardProps) {
  const filteredCards = cards.filter(
    (card) =>
      card.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      card.description.toLowerCase().includes(searchTerm.toLowerCase())
  )

  return (
    <div className="board">
      {filteredCards.map((card) => (
        <CardComponent
          key={card.id}
          card={card}
          allCards={cards}
          onUpdate={onUpdate}
          searchTerm={searchTerm}
        />
      ))}
    </div>
  )
}

function App() {
  const [cards, setCards] = useState<Card[]>(() => generateCards(500))
  const [searchTerm, setSearchTerm] = useState('')

  const updateCard = (id: number, newData: Card) => {
    setCards(cards.map((c) => (c.id === id ? newData : c)))
  }

  const filteredCount = cards.filter(
    (c) =>
      c.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      c.description.toLowerCase().includes(searchTerm.toLowerCase())
  ).length

  return (
    <div className="app">
      <div className="header">
        <h1>Story Board</h1>
        <p className="subtitle">
          This board has 500 story cards. Try scrolling, typing in search, and
          clicking update buttons.
        </p>

        <div className="search-container">
          <label>Search Cards:</label>
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            placeholder="Search by title or description..."
            className="search-input"
          />
        </div>

        <div className="card-count">
          Showing {filteredCount} of {cards.length} cards
        </div>
      </div>

      <Board cards={cards} onUpdate={updateCard} searchTerm={searchTerm} />
    </div>
  )
}

export default App
